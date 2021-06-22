# Payload dumps from Collaboratory SONG server

This folder contains SONG payload dumps from Collaboratory. The dumps are retrieved using `curl` get
requests, an example is given as below:

```
curl -XGET 'https://song.cancercollaboratory.org/studies/CLLE-ES/analysis?analysisStates=PUBLISHED' |gzip - > CLLE-ES.payloads.2021-06-21.json.gz
```
