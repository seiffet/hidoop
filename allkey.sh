#!/bin/bash


montableau=(
            "brochet"
            "carpe"
            "chevesne"
            "gardon"
            "goujon"
            "omble"
            "perche"
            "tanche"
            "truite"
            "sandre"
            "silure"
            "vairon"
            "maya"
            "goldorak"
            "candy"
            "snorki"
            "albator"
            "bouba"
            "heidi"
            "calimero"
            "casimir"
            "esteban"
            "zia"
            "tao"
            "clementine"
            "diabolo"
            "scoubidou"
            "ladyoscar"
            "alose"
            "bonite"
            "corb"
            "daurade"
            "eperlan"
            "fletan"
            "gobie"
            "mordocet"
            "nerophis"
            "orphie"
            "palomete"
            "rouget"
            "sar"
            "tacaud"
            "luke"
            "leia"
            "r2d2"
            "vador"
            "solo"
            "palpatine"
            "yoda"
            "kenobi"
            "ewok"
            "jabba"
            "chewie"
            "lando"
            "dagobah"
            "bobafett"
            "z6po"
            "ackbar"
            "atkinson"
            "atanasoff"
            "babagge"
            "backus"
            "boole"
            "carmack"
            "colmerauer"
            "dijkstra"
            "gates"
            "gosling"
            "grove"
            "hopper"
            "jobs"
            "kernighan"
            "knuth"
            "liskov"
            "lovelace"
            "moore"
            "murdock"
            "neumann"
            "shannon"
            "stallman"
            "swartz"
            "torvalds"
            "turing"
            "zemanek"
            "angel"
            "banshee"
            "bishop"
            "cable"
            "colossus"
            "cyclope"
            "dazzler"
            "diablo"
            "forge"
            "gambit"
            "havok"
            "iceberg"
            "jeangrey"
            "jubele"
            "magma"
            "magneto"
            "malicia"
            "mystique"
            "polaris"
            "psylocke"
            "rocket"
            "serval"
            "shadowcat"
            "sunfire"
            "tornade"
            "xorn"
            "arryn"
            "baratheon"
            "bravoos"
            "dorne"
            "greyjoy"
            "lannister"
            "marcheursblancs"
            "martell"
            "meeren"
            "portreal"
            "sauvageons"
            "stark"
            "targaryen"
            "tully"
            "tyrell"
            "volantis"
            "winterfell"
            "archimede"
            "aston"
            "copernic"
            "darwin"
            "descartes"
            "edison"
            "einsten"
            "fermat"
            "galilee"
            "hawking"
            "kepler"
            "newton"
            "ohm"
            "pascal"
            "socrate"
            "tesla"
            "volta"
            "watt"
            "apollinaire"
            "baudelaire"
            "brassens"
            "demusset"
            "ferre"
            "gautier"
            "hugo"
            "lafontaine"
            "lamartine"
            "mallarme"
            "maupassant"
            "poe"
            "prevert"
            "rimbaud"
            "sand"
            "verlaine"
            "aragorn"
            "arwen"
            "bilbo"
            "boromir"
            "elrond"
            "eomer"
            "eowyn"
            "faramir"
            "frodon"
            "galadriel"
            "gandalf"
            "gimli"
            "legolas"
            "merry"
            "pippin"
            "sam"
            "aspicot"
            "bulbizarre"
            "carapuce"
            "chenipan"
            "hypotrempe"
            "magicarpe"
            "melofee"
            "nidoran"
            "piafabec"
            "pikachu"
            "psykokwak"
            "rattata"
            "rondoudou"
            "roucool"
            "salameche"
            "taupiqueur"
            "ader"
            "bastie"
            "bleriot"
            "boeing"
            "boucher"
            "farman"
            "garros"
            "guynemer"
            "latecoere"
            "lindbergh"
            "marvingt"
            "mermoz"
            "messerschmitt"
            "saintexupery"
            "voisin"
            "wright"
            "acdc"
            "aerosmith"
            "beatles"
            "clapton"
            "clash"
            "cooper"
            "deeppurple"
            "doors"
            "dylan"
            "eagles"
            "epica"
            "hendrix"
            "kiss"
            "ledzepplin"
            "loureed"
            "motorhead"
            "metallica"
            "muse"
            "pinkfloyd"
            "queen"
            "scorpion"
            "stones"
            "superbus"
            "zztop")

len=${#montableau[@]}


ssh-keygen -t  rsa;

perl -e "print '$2'" > stdin ;


for (( i=1; i<$len; i++ ));
do

ssh-copy-id $1@${montableau[$i]} < stdin


done