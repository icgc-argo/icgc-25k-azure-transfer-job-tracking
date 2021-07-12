# Payload dumps from Collaboratory SONG server

This folder contains SONG payload dumps from Collaboratory. The dumps are retrieved using `curl` get
requests, an example is given as below:

```
curl -XGET 'https://song.cancercollaboratory.org/studies/CLLE-ES/analysis?analysisStates=PUBLISHED' |gzip - > CLLE-ES.payloads.2021-06-21.json.gz
```

List of studies:
```
BOCA-UK
BRCA-UK
BTCA-SG
CLLE-ES
CMDI-UK
ESAD-UK
LAML-KR
LICA-FR
LINC-JP
LIRI-JP
MELA-AU
ORCA-IN
OV-AU
PACA-AU
PACA-CA
PAEN-AU
PAEN-IT
PRAD-CA
PRAD-UK
RECA-EU
```
