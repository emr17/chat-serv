This is the default location for the server certs. Change the path in config.js

generate new self signed key and cert with
openssl req -newkey rsa:2048 -new -nodes -x509 -days 3650 -keyout key.pem -out cert.pem
