# DISC-12: TLS certifikaty pro on-premise / factory prostredi

## Zdroje

- https://docs.nginx.com/nginx/admin-guide/security-controls/terminating-ssl-http/ — Nginx TLS terminace
- https://docs.eu1.edge.siemens.cloud/get_started_and_operate/industrial_edge_management/k8s/operation/tls_certificates/certificates.html — Siemens Industrial Edge TLS
- https://learn.microsoft.com/en-us/azure/iot-edge/how-to-manage-device-certificates — Azure IoT Edge certs
- https://infisical.com/blog/certificate-management — Certificate management guide

## Relevance: STREDNI

## Klicove poznatky

### Varianty certifikatu pro on-premise

1. **Self-signed certifikat**
   - Generovany lokalne (openssl)
   - Zdarma, zadna zavislost na externich sluzbch
   - Edge worker musi mit root CA v trust store
   - Manualni renewal (typicky 1 rok)

2. **Interni CA (firemni)**
   - Vlastni certifikacni autorita (napr. Microsoft AD CS, step-ca, cfssl)
   - Automaticke vydavani certifikatu pro interni sluzby
   - Edge zarizeni duveruji interni CA (deployovana pres Group Policy nebo manualne)
   - Vhodne pro vetsi instalace (10+ edge zarizeni)

3. **Let's Encrypt**
   - Zdarma, automaticke obnoveni (certbot)
   - ALE: vyzaduje verejnou domenu a pristup z internetu
   - Pro interni factory sit **nepouzitelne** (servery nemaji verejnou IP)

### Prumyslova praxe (Siemens, Azure IoT)

- Komunikace edge ↔ management = TLS povinne
- Certifikat chain: root CA → intermediate → server cert
- Edge zarizeni ma root CA v trust store
- Automaticke renewal kde to jde (IoT Hub, cert-manager)

### Pro nas use case

- Factory floor = interni sit, zadna verejna IP
- Let's Encrypt nepouzitelne
- Self-signed = nejjednodussi pro zacatek
- Interni CA = pro produkci pokud zakaznik ma AD/PKI infrastrukturu
