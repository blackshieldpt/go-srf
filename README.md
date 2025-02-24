# go-srf

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![Go version](https://img.shields.io/badge/Go-≥1.22.2-blue)](https://go.dev/)


SRF golang library

## What is SRF?

SRF, aka **S**imple **R**ecord **F**ormat, is a container file format, that can be used to store arbitrary data - text, 
binary data, JSON objects, etc. Each item of data is stored in a structure called Record, with a 20 byte header, plus
optional arbitrary metadata in JSON format; The JSON metadata - if present - is always stored in compressed form using
zstandard; The Record data, i.e. the actual data to be stored, can also be stored in a compressed format.

Please refer to the [SRF spec](https://github.com/blackshieldpt/srf-spec) for more details on the format.

## Usage
Use ```go get github.com/blackshieldpt/go-srf@latest``` to add it to your project.

## Examples
```go
```

## Dependencies

go-spf relies on the excellent [zstandard compress](https://github.com/klauspost/compress) library to perform 
compression and decompression. Please check the library page for licensing information.
