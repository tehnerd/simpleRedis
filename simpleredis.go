package simpleredis

import (
	utils "github.com/tehnerd/goUtils"
	"net"
	"strconv"
	"strings"
	"time"
)

type RedisCmd struct {
	Command string
	Name    string
	Data    []byte
}

func GenRedisArray(params ...[]byte) []byte {
	CRLF := "\r\n"
	MSG := strings.Join([]string{"*", strconv.Itoa(len(params)), CRLF}, "")
	for cntr := 0; cntr < len(params); cntr++ {
		Section := strings.Join([]string{"$", strconv.Itoa(len(params[cntr])),
			CRLF, string(params[cntr]), CRLF}, "")
		MSG = strings.Join([]string{MSG, Section}, "")
	}
	return []byte(MSG)
}

func RedisSet(name string, data []byte) []byte {
	return GenRedisArray([]byte("SET"), []byte(name), data)
}

func RedisGet(name string) []byte {
	return GenRedisArray([]byte("GET"), []byte(name))
}

func ParseRedisResponse(response []byte, dataBuf []byte, Len *int) ([]byte, []byte) {
	dataBuf = append(dataBuf, response...)
	if *Len != 0 {
		if len(dataBuf) < *Len {
			return nil, dataBuf
		} else {
			return dataBuf[:*Len], dataBuf[:*Len]
		}
	}
	for {
		switch string(dataBuf[0]) {
		case "+", "-", ":":
			//simple strings, error,int. usually ther are in format (+|-|:)DATA\r\n"
			if len(dataBuf) < 3 {
				return nil, dataBuf
			}
			cntr := 1
			for ; cntr < len(dataBuf); cntr++ {
				if dataBuf[cntr] == '\r' {
					break
				}
			}
			if cntr == len(dataBuf) {
				return nil, dataBuf
			}
			response = dataBuf[1:cntr]
			return response, dataBuf[cntr+2:]
		case "$":
			//bulk string. format $LEN\r\nDATA\r\n. up to 512MB
			cntr := 1
			for ; cntr < len(dataBuf); cntr++ {
				if string(dataBuf[cntr]) == "\r" {
					break
				}
			}
			if cntr == len(dataBuf) {
				return nil, dataBuf
			}
			dataLen, err := strconv.Atoi(string(dataBuf[1:cntr]))
			if err != nil {
				return nil, dataBuf[cntr:]
			}

			if dataLen == -1 {
				return []byte("NOT FOUND"), dataBuf[cntr:]
			}
			if len(dataBuf[cntr:len(dataBuf)-2]) < dataLen {
				*Len = dataLen
				return nil, dataBuf[cntr:]
			} else {
				return dataBuf[cntr : cntr+dataLen], dataBuf[cntr+dataLen:]
			}
		case "*":
			panic("array")
		default:
			if len(dataBuf) > 1 {
				dataBuf = dataBuf[1:]
			} else {
				return nil, dataBuf
			}
		}
	}
	return nil, dataBuf
}

func RedisContext(hostnamePort string, redisCmd chan RedisCmd) {
	tcpRemoteAddress, err := net.ResolveTCPAddr("tcp", hostnamePort)
	if err != nil {
		panic("cant resolve remote redis address")
	}
	var ladr *net.TCPAddr
	msgBuf := make([]byte, 65000)
	initMsg := []byte("*1\r\n$4\r\nPING\r\n")
	writeChan := make(chan []byte)
	readChan := make(chan []byte)
	flushChan := make(chan int)
	go utils.AutoRecoonectedTCP(ladr, tcpRemoteAddress, msgBuf,
		initMsg, writeChan, readChan, flushChan)
	<-readChan
	loop := 1
	dataBuf := make([]byte, 0)
	dataLen := 0
	for loop == 1 {
		select {
		case cmd := <-redisCmd:
			switch cmd.Command {
			case "SET":
				data := RedisSet(cmd.Name, cmd.Data)
				writeChan <- data
			case "GET":
				data := RedisGet(cmd.Name)
				writeChan <- data
			}
		case response := <-readChan:
			data, dataBuf := ParseRedisResponse(response, dataBuf, &dataLen)
			if dataLen != 0 {
				for data == nil {
					response = <-readChan
					data, dataBuf = ParseRedisResponse(response, dataBuf, &dataLen)
				}
			}
			if data != nil && (len(data) != 4 || string(data) != "PONG") {
				var responseData RedisCmd
				responseData.Data = data
				redisCmd <- responseData
				dataLen = 0
			}
		case <-flushChan:
			dataBuf = dataBuf[:]
			dataLen = 0
			select {
			case redisCmd <- RedisCmd{}:
			case <-time.After(time.Second * 5):
			}
		}
	}
}
