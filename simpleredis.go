package simpleredis

import (
	utils "github.com/tehnerd/goUtils"
	"net"
	"strconv"
	"strings"
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
	if *Len != 0 {
		dataBuf = append(dataBuf, response...)
		if len(dataBuf) < *Len {
			return nil, dataBuf
		} else {
			return dataBuf[:*Len], dataBuf
		}
	}
	switch string(response[0]) {
	case "+", "-", ":":
		//simple strings, error,int. usually ther are in format (+|-|:)DATA\r\n"
		if len(response) < 3 {
			return nil, dataBuf
		}
		response = response[1 : len(response)-2]
		return response, dataBuf
	case "$":
		//bulk string. format $LEN\r\nDATA\r\n. up to 512MB
		cntr := 1
		for ; cntr < len(response); cntr++ {
			if string(response[cntr]) == "\r" {
				cntr += 2
				break
			}
		}
		dataLen, err := strconv.Atoi(string(response[1 : cntr-2]))
		if err != nil {
			return nil, dataBuf
		}
		if dataLen == -1 {
			return nil, dataBuf
		}
		if cntr > len(response) || cntr > len(response)-2 {
			*Len = dataLen
			return nil, dataBuf
		}
		if len(response[cntr:len(response)-2]) < dataLen {
			dataBuf = append(dataBuf, response[cntr:]...)
			*Len = dataLen
			return nil, dataBuf
		} else {
			return response[cntr : cntr+dataLen], dataBuf
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
	go utils.AutoRecoonectedTCP(ladr, tcpRemoteAddress, msgBuf,
		initMsg, writeChan, readChan)
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
				dataBuf = dataBuf[:]
			}
		}
	}
}
