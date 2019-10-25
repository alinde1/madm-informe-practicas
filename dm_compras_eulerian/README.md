**Description**

Send booking info to Eulerian.

**Source**

*S3 bucket*

gzip files stored in S3.

`s3://bucket-cdr-landing-live/CRM/Salesforce/Reservas/reservas{year}/{month}/{day}/{hour}/*.gz`

*Available Fields*

The following is a list of all possible fields a booking can have. Only non null ones are present in the `JSON` file.

```
locator
superLocator
status
statusContactCenter
hotelId
checkIn
checkOut
posCode
pos
sgaCode
sga
countryCode
provinceCode
agencyId
enterpriseId
fareCode
fare
cardGroup
cardNumber
customerId
guestName
grossAmount
netAmount
currencyCode
points
limitDate
roomTypeCode
roomType
issueId
promoCode
createdUserId
createdUserName
createdDate
modifiedUserId
modifiedUserName
modifiedDate
idANIWallmeric
euroNetAmount
baseCurrency
indicadorHayIdANI
numeroNochesReserva
totalAdultosReserva
totalNinosReserva
cardGroupIntermediary
cardNumberIntermediary
tipoDispositivoOrigenReserva
formaPago
descripcionFormaPago
cantidadPrepagada
porcentajeComisionAgencia
porcentajeTasasLocales
importeTasasLocales
indicadorTasasLocalesIncluidas
observaciones1old
observaciones2old
indicadorWebCheckin
campoReferenciaReserva
tipoNormaGarantiaRATNV
descripcionTipoGarantia
tipoTarjetaCreditoGarantia
titularTarjetaCreditoGarantia
numeroEnmascaradoTarjetaCreditoGarantia
fechaExpiracionTarjetaCreditoGarantia
bonoGarantia
porcentajeDeposito
cantidadDeposito
monedaDeposito
nochesDeposito
tiempoAntesLlegadaDeposito
tiempoDespuesLlegadaDeposito
unidadTiempoDeposito
nombreBancoDeposito
numeroCuentaBancoDeposito
titularCuentaBanco
horaLimite
politicaCancelacion1
politicaCancelacion2
politicaCancelacion3
politicaCancelacion4
politicaCancelacion5
motivoCancelacion
textoIndicaSiReservaTratadoComoNoShow
roomNightsReserva
codigoClienteIntermediario
descuentoNeto
descuentoNetoEUR
estadoPreReserva
``` 

*Sample Data*

```
{"locator": "1111111111",
 "status": "R",
 "statusContactCenter": "R",
 "hotelId": "6393",
 "checkIn": "2019-10-28",
 "checkOut": "2019-11-01",
 "numeroNochesReserva": "4",
 "roomNightsReserva": "4",
 "totalAdultosReserva": "1",
 "posCode": "98556",
 "pos": "G.D.S. SABRE",
 "fareCode": "NG1",
 "sgaCode": "AA",
 "sga": "SABRE",
 "createdUserId": "SABRE",
 "createdUserName": "USER",
 "createdDate": "2019-09-11T10:34:04.000+02:00",
 "formaPago": "PD",
 "descripcionFormaPago": "PAGA CLIENTE",
 "roomTypeCode": "C1K",
 "roomType": "INNSIDE KING GUESTROOM",
 "tipoNormaGarantiaRATNV": "H",
 "monedaDeposito": "GBP",
 "horaLimite": "1800",
 "politicaCancelacion1": "Si cancela después de 6 PM del dia de llegada o no se presenta, 1 Noche(s) de cargo como gastos de cancelación",
 "textoIndicaSiReservaTratadoComoNoShow": "N",
 "grupoValoracionDiaria": [{"numeroHabitacionPertenece": "1",
   "codigoServicio": "KB1",
   "fechaServicioReservado": "2019-10-28",
   "cantidadServicioEseDia": "1",
   "precioServicioEseDia": "100.00",
   "tarifaDiariaServicioEseDia": "NG1"},
  {"numeroHabitacionPertenece": "1",
   "codigoServicio": "KB1",
   "fechaServicioReservado": "2019-10-29",
   "cantidadServicioEseDia": "1",
   "precioServicioEseDia": "100.00",
   "tarifaDiariaServicioEseDia": "NG1"},
  {"numeroHabitacionPertenece": "1",
   "codigoServicio": "KB1",
   "fechaServicioReservado": "2019-10-30",
   "cantidadServicioEseDia": "1",
   "precioServicioEseDia": "100.00",
   "tarifaDiariaServicioEseDia": "NG1"},
  {"numeroHabitacionPertenece": "1",
   "codigoServicio": "KB1",
   "fechaServicioReservado": "2019-10-31",
   "cantidadServicioEseDia": "1",
   "precioServicioEseDia": "100.00",
   "tarifaDiariaServicioEseDia": "NG1"}],
 "grupoTasas": [{"porcentajeTasa": "20.00",
   "importeTasa": "66.67",
   "indicadorTasaIncluida": "S"}]}
```

**Mapping**

The following table describe the mapping between booking fields and Eulerian API parameters.

* **params**: Eulerian API parameter. Required parameters are in boldface.
* **json**: Booking fields. These are the fields included in the json file. Some are optional and may not be included.
there are cases where a calculation is used instead of a direct mapping.
* **fixed**: Fixed value. Set the parameter to this value.
 
|params|json|fixed|
|----|----|----|
|**ref**|locator| |
|**amount**|euroNetAmount| | 
|**ereplay-time**| createdDate in Epoch UNIX format||
|from| |com|
|type|pos| |
|currency| |"EUR" |
|payment|descripcionFormaPago| | 
|uid|customerId| |
|profile| |buyer |
|prdref|hotelId| |
|prdamount|euroNetAmount| | 
|prdquantity| |1 |
|user_country_ISO_Code|countryCode| | 
|user_type|cardGroup| |
|userRewardsType|cardGroup| | 
|order_checkin|checkIn (yyyymmdd)| | 
|order_checkout|checkOut (yyyymmdd)| | 
|order_checkin_month|checkIn (mm)| |
|order_checkin_year|checkIn (yy)| |
|order_checkinWeekDay|checkin weekday| | 
|order_Type|descripcionFormaPago| |
|order_adults|totalAdultosReserva| |
|order_children|totalNinosReserva| |
|order_daysinAdvance|createdDate - checkIn in days| | 
|order_nights|numeroNochesReserva| |
|order_rooms|roomNightsReserva / numeroNochesReserva | |
|order_room_code|roomTypeCode|  |
|order_hotel_room|roomTypeCode| |
|order_rate|fareCode| |

**Destination**

Eulerian API.

Used endpoints:

* [replay.json](https://doc.api.eulerian.com/#tag/Datamining:-sales%2Fpaths%2F~1ea~1%7Bsite%7D~1report~1order~1replay.json%2Fget)
* [cancel.json](https://doc.api.eulerian.com/#tag/Datamining:-sales%2Fpaths%2F~1ea~1%7Bsite%7D~1report~1order~1cancel.json%2Fpost)
* [valid.json](https://doc.api.eulerian.com/#tag/Datamining:-sales%2Fpaths%2F~1ea~1%7Bsite%7D~1report~1order~1cancel.json%2Fpost)

**Jira**


**Confluence**


**Airflow**

* 
