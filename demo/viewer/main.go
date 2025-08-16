package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"time"
)

var (
	subscribers   = make(map[chan []byte]struct{})
	subscribersMu sync.Mutex
)

func main() {
	http.Handle("/", http.FileServer(http.Dir("demo/viewer/static")))
	http.HandleFunc("/sse", serveSSE)
	http.HandleFunc("/broadcast", broadCast)

	port := "8081"
	log.Println("Server running at http://localhost:" + port)

	err := http.ListenAndServe(":"+port, nil)
	fmt.Println("server error", err)
}

func serveSSE(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}
	msgCh := make(chan []byte, 10)
	subscribersMu.Lock()
	subscribers[msgCh] = struct{}{}
	subscribersMu.Unlock()

	defer func() {
		subscribersMu.Lock()
		defer subscribersMu.Unlock()
		delete(subscribers, msgCh)
		close(msgCh)
	}()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		// case <-ticker.C:
		// 	fmt.Fprintf(w, "data: %s\n\n", time.Now())
		// 	flusher.Flush()
		case msg := <-msgCh:
			fmt.Println(string(msg))
			fmt.Fprintf(w, "data: %s\n\n", string(msg))

			flusher.Flush()
		}
	}
}

func broadCast(w http.ResponseWriter, r *http.Request) {
	subscribersMu.Lock()
	for ch := range subscribers {
		b, _ := io.ReadAll(r.Body)
		ch <- b
	}
	subscribersMu.Unlock()
	fmt.Fprintf(w, "ok")
}
