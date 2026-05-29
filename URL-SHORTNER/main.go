package main

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"
)

type URL struct {
	ID string `json:"id"`
	OriginalURL string `json:"original_url"`
	ShortURL string `json:"short_url"`
	CreationDate time.Time `json: "creation_date"`

}
var urlDB = make(map[string]URL)

func generateShortURL(originalURL string) string{
	hasher := md5.New()
	hasher.Write([]byte(originalURL))
	data := hasher.Sum(nil);
	hash := hex.EncodeToString(data)
	return hash[:8]
}

func createURL(originalURL string) string {
	shortURL := generateShortURL(originalURL)
	id := shortURL
	urlDB[id] = URL{
		ID: id,
		OriginalURL: originalURL,
		ShortURL: shortURL,
		CreationDate: time.Now(),
	}
	return id
}

func getURL(id string) (URL,error) {
	url, ok := urlDB[id]
	if !ok {
		return URL{}, errors.New("URL not found")
	}
	return url, nil
}

func handler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "hello world")
}

func ShortURLHandler(w http.ResponseWriter, r *http.Request) {
	var data struct {
		URL string `json:"url"`
	}

	err := json.NewDecoder(r.Body).Decode(&data)
	if err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	shortURL_ := createURL(data.URL)
	response := struct {
		ShortURL string `json:"short_url"`
	}{
		ShortURL: shortURL_,
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func redirectURLHandler(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Path[len("/redirect/"):]
	url,err := getURL(id)
	if err != nil {
		http.Error(w, "Invalid Request", http.StatusNotFound)
	}

	http.Redirect(w,r,url.OriginalURL, http.StatusFound)

}

func main() {

	http.HandleFunc("/", handler)
	http.HandleFunc("/shorten", ShortURLHandler)
	http.HandleFunc("/redirect/", redirectURLHandler)

	fmt.Println("Server start on port 3457")
	err := http.ListenAndServe(":3457",nil)
	if err != nil {
		fmt.Println("Error on starting server", err)
	}
}