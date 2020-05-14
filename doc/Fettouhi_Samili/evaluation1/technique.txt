Pour faire fonctionné ce programme il vaut mieux utilisé eclipse 
pour évité des érreurs "fichiers introuvable" puisque c'est différent.

on cas de compilation par javac , if faut absolution changé les endrois (Class.Project.PATH) pour pouvoir trouvé les fichiers
dans le cas de eclipse "./data/" suffit ms pour javac , if faut spécifier les endroits (Project.PATH = "./" donc if faut un folder "data" poue ne pas avoir des érreurs)

pour testé

lanncé premièrement le DaemonMaster avec un paramètre le port : 9999

exmeple java DaemonMasterImpl 9999

2 - lancer un DaemonImpl qui prend les arguments(port,numdaemon) dans le port 10000 0

3 - lancer un DaemonImpl qui prend les arguments(port,numdaemon) dans le port 10001 1

4 - lancer un DaemonImpl qui prend les arguments(port,numdaemon) dans le port 10002 2

5 - pour faire le test lancer MyMapReduce 


Commentaire partie Technique :


la gestion des fichiers n'est très exacte puis c'est HDFS qui s'occupe donc dans notre cas on a pas bien précisé l'endroits des fichiers puisque toutes sont dans localhost (donc pas de socket pour l'instant)
 la chose qui n'est pas tres efficace dans un problème avec une taille de données masive.

le résultats des calcule de chuque étape est enregistrer dans des fichiers temp donc pour amélioration il faut faire un sripts qui les supprime.


pour résons de test quelque fonctions ne sont pas utilisé nbreducers/test si le fichiers d'entre est de formats KV ou LIGN toutes est commanté dans dans le code.
chaque est class est résponsable a une tache ou sous tache comme son nom indique, a l'execéption de class DaemonMaster qui a pour objectif avoir une trace sur les résultats des Daemons.

la class de comparaiseon des String n'est  pas utilisé pour l'instant.

des amélioration envisagé : 

utilisé des thread dans les reducers pour avoire plus de parallélisme.

des amélioration au niveau gestion de fichiers.
