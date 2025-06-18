# mkcert

[mkcert](https://github.com/FiloSottile/mkcert)

```
mkcert -install
mkcert fbs.forbes.com
```

# openssl

## Generate a private key

```
openssl genrsa -out server.key 2048
```

## Generate a CSR

```
openssl req -new -key server.key -out server.csr
```

## Self-sign the cert

```
openssl x509 -req -days 365 -in server.csr -signkey server.key -out server.crt