package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"os"
	"time"

	"github.com/cheggaaa/pb/v3"
)

func exit(msg string) {
	fmt.Println(msg)
	os.Exit(1)
}

func main() {
	if len(os.Args) < 2 {
		exit("Needs an argument")
	}
	if os.Args[1] == "-l" {
		if len(os.Args) < 3 {
			exit("Needs filename argument")
		}
		if err := listen(os.Args[2:]); err != nil {
			exit(err.Error())
		}
	} else {
		for {
			if err := connect(os.Args[1]); err != nil {
				fmt.Println("Error:", err)
				fmt.Println("Reconnecting...")
				time.Sleep(2 * time.Second)
				continue
			}
			break
		}
	}
}

const port = ":3333"
const chunkSize = 64 * 1024

func listen(filenames []string) (err error) {
	fmt.Print("Calculating hashes...")
	hashes := make([][]uint32, len(filenames))
	for i, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			return err
		}
		hashes[i], err = calcHashes(file)
		file.Close()
		if err != nil {
			return err
		}
	}
	fmt.Println(" done")
	ln, err := net.Listen("tcp", port)
	if err != nil {
		return
	}
	defer ln.Close()
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go handleConnection(conn, filenames, hashes)
	}
}

func calcChunkSize(chunkIndex int, fileSize int64) int64 {
	if int64(chunkIndex+1)*chunkSize > fileSize {
		return fileSize % chunkSize
	}
	return chunkSize
}

func calcHashes(file *os.File) (hashes []uint32, err error) {
	stat, err := file.Stat()
	if err != nil {
		return
	}
	size := stat.Size()
	if _, err = file.Seek(0, 0); err != nil {
		return
	}
	chunks := (size + chunkSize - 1) / chunkSize
	hashes = make([]uint32, chunks)
	for i := range hashes {
		h := crc32.NewIEEE()
		if _, err = io.CopyN(h, file, calcChunkSize(i, size)); err != nil {
			return
		}
		hashes[i] = h.Sum32()
	}
	return
}

func handleConnection(conn net.Conn, filenames []string, hashes [][]uint32) (err error) {
	defer conn.Close()
	if err = binary.Write(conn, binary.LittleEndian, int32(len(filenames))); err != nil {
		return
	}
	for i, filename := range filenames {
		if err = sendFile(conn, filename, hashes[i]); err != nil {
			return
		}
	}
	return
}

func sendFile(conn net.Conn, filename string, hashes []uint32) (err error) {
	file, err := os.Open(filename)
	if err != nil {
		return
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return
	}
	size := stat.Size()
	if _, err = fmt.Fprintln(conn, stat.Name()); err != nil {
		return
	}
	if err = binary.Write(conn, binary.LittleEndian, size); err != nil {
		return
	}
	if err = binary.Write(conn, binary.LittleEndian, hashes); err != nil {
		return
	}
	hashOk := make([]bool, len(hashes))
	if err = binary.Read(conn, binary.LittleEndian, hashOk); err != nil {
		return
	}
	for i := range hashOk {
		if !hashOk[i] {
			if _, err = file.Seek(int64(i*chunkSize), 0); err != nil {
				return
			}
			if _, err = io.CopyN(conn, file, calcChunkSize(i, size)); err != nil {
				return
			}
		}
	}
	return
}

func connect(server string) (err error) {
	conn, err := net.Dial("tcp", server+port)
	if err != nil {
		return
	}
	defer conn.Close()
	r := bufio.NewReader(conn)
	var n int32
	if err = binary.Read(r, binary.LittleEndian, &n); err != nil {
		return
	}
	for ; n > 0; n-- {
		if err = recvFile(r, conn); err != nil {
			return
		}
	}
	return
}

func recvFile(r *bufio.Reader, w io.Writer) (err error) {
	filename, err := r.ReadString('\n')
	if err != nil {
		return
	}
	filename = filename[:len(filename)-1]
	var size int64
	if err = binary.Read(r, binary.LittleEndian, &size); err != nil {
		return
	}
	fmt.Printf("%s, size = %d bytes\n", filename, size)
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return
	}
	defer file.Close()
	if err = file.Truncate(size); err != nil {
		return
	}
	chunks := (size + chunkSize - 1) / chunkSize
	hashes := make([]uint32, chunks)
	hashes2, err := calcHashes(file)
	if err != nil {
		return
	}
	if err = binary.Read(r, binary.LittleEndian, hashes); err != nil {
		return
	}
	hashOk := make([]bool, chunks)
	for i := range hashes2 {
		hashOk[i] = hashes[i] == hashes2[i]
	}
	if err = binary.Write(w, binary.LittleEndian, hashOk); err != nil {
		return
	}
	bar := pb.StartNew(int(chunks))
	for i := range hashOk {
		if !hashOk[i] {
			if _, err = file.Seek(int64(i*chunkSize), 0); err != nil {
				return
			}
			h := crc32.NewIEEE()
			if _, err = io.CopyN(file, io.TeeReader(r, h), calcChunkSize(i, size)); err != nil {
				return
			}
			if h.Sum32() != hashes[i] {
				return errors.New("Hash mismatch")
			}
		}
		bar.Increment()
	}
	bar.Finish()
	return
}
