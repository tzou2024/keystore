package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Println(r.Method, r.RequestURI)
		next.ServeHTTP(w, r)
	})
}

func notAllowedHandler(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "Not Allowed", http.StatusMethodNotAllowed)
}

func keyValuePutHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	value, err := io.ReadAll(r.Body)
	defer r.Body.Close()
	//if theres an error reading the value, respond server error
	if err != nil {
		http.Error(w,
			err.Error(),
			http.StatusInternalServerError)

		return
	}

	err = Put(key, string(value))
	// if theres an error putting, respond server error
	if err != nil {
		http.Error(w,
			err.Error(),
			http.StatusInternalServerError)
		return
	}
	fmt.Println("writing: ", string(value))
	transact.WritePut(key, string(value))
	// if everthing went well, return status created
	w.WriteHeader(http.StatusCreated)
	log.Printf("PUT key=%s value=%s\n", key, string(value))
}

func keyValueGetHandler(w http.ResponseWriter, r *http.Request) {
	//pull variabes
	vars := mux.Vars(r)
	//get the key
	key, ok := vars["key"]

	var err error
	// if you can't find the value, return an error
	if !ok {
		http.Error(w,
			err.Error(),
			http.StatusNotFound)
		return
	}
	var val string

	val, err = Get(key)

	//if the err was a no such key let them know

	if errors.Is(err, ErrorNoSuchKey) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	// if the error was something else report internal
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// if everything went well, retrn the val
	w.Write([]byte(val))

	log.Printf("GET key=%s\n", key)
}

func keyValueDeleteHandler(w http.ResponseWriter, r *http.Request) {
	vals := mux.Vars(r)

	key := vals["key"]

	err := Delete(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	transact.WriteDelete(key)

	log.Printf("DELETE key=%s\n", key)

}

func helloMuxHandler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("Hey gmux"))
}

func main() {
	err := initalizeTransactionLog()
	if err != nil {
		panic(err)
	}
	r := mux.NewRouter()

	r.Use(loggingMiddleware)
	// Register keyValuePutHandler as the handler function for PUT
	// requests matching "/v1/{key}"
	r.HandleFunc("/v1/{key}", keyValuePutHandler).Methods("PUT")
	r.HandleFunc("/v1/{key}", keyValueGetHandler).Methods("GET")
	r.HandleFunc("/v1/{key}", keyValueDeleteHandler).Methods("DELETE")

	r.HandleFunc("/v1", notAllowedHandler)
	r.HandleFunc("/v1/{key}", notAllowedHandler)
	r.HandleFunc("/", helloMuxHandler)
	log.Fatal(http.ListenAndServe(":8080", r))
}
