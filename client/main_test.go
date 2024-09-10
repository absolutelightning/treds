package main

import (
	"bufio"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

// MockConn is a mock implementation of net.Conn interface for testing
type MockConn struct {
	readData  string
	writeData string
}

func (m *MockConn) Read(b []byte) (n int, err error) {
	copy(b, m.readData)
	return len(m.readData), io.EOF
}

func (m *MockConn) Write(b []byte) (n int, err error) {
	m.writeData = string(b)
	return len(b), nil
}

func (m *MockConn) Close() error                       { return nil }
func (m *MockConn) LocalAddr() net.Addr                { return nil }
func (m *MockConn) RemoteAddr() net.Addr               { return nil }
func (m *MockConn) SetDeadline(t time.Time) error      { return nil }
func (m *MockConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *MockConn) SetWriteDeadline(t time.Time) error { return nil }

// TestConnectToTreds tests the connectToTreds function
func TestConnectToTreds(t *testing.T) {
	_, err := connectToTreds("invalid-address")
	if err == nil {
		t.Error("Expected an error for invalid address")
	}
}

// TestSendCommand tests the sendCommand function
func TestSendCommand(t *testing.T) {
	mockConn := &MockConn{}
	err := sendCommand(mockConn, "TEST COMMAND")
	if err != nil {
		t.Errorf("sendCommand failed: %v", err)
	}

	if mockConn.writeData != "TEST COMMAND" {
		t.Errorf("Expected 'TEST COMMAND', got '%s'", mockConn.writeData)
	}
}

// TestReadUntilNewline tests the readUntilNewline function
func TestReadUntilNewline(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("Hello\nWorld"))
	line, err := readUntilNewline(reader)
	if err != nil {
		t.Errorf("readUntilNewline failed: %v", err)
	}

	expected := "Hello\n"
	if line != expected {
		t.Errorf("Expected '%s', got '%s'", expected, line)
	}
}

// TestReadFixedBytes tests the readFixedBytes function
func TestReadFixedBytes(t *testing.T) {
	reader := bufio.NewReader(strings.NewReader("HelloWorld"))
	data, err := readFixedBytes(reader, 5)
	if err != nil {
		t.Errorf("readFixedBytes failed: %v", err)
	}

	expected := "Hello"
	if string(data) != expected {
		t.Errorf("Expected '%s', got '%s'", expected, string(data))
	}
}

// TestReadAllData tests the readAllData function
func TestReadAllData(t *testing.T) {
	mockConn := &MockConn{readData: "5\nHello"}
	data, err := readAllData(mockConn)
	if err != nil {
		t.Errorf("readAllData failed: %v", err)
	}

	expected := "Hello"
	if data != expected {
		t.Errorf("Expected '%s', got '%s'", expected, data)
	}
}
