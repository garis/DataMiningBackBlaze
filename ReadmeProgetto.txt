‣Esecuzione:
Settare in Utils.java il path facendolo puntare alla cartella contenente tutti i file .csv del dataset.
Decommentare in fondo al metodo Main i metodi da usare per compiere le analisi.
Eseguire il codice, tutti i file intermedi come ad esempio lastDay.csv, failedDisks.csv e altri file temporanei
verranno gestiti in automatico ma, se necessario, essi possono essere semplicemente cancellati in caso di problemi.

‣Main.java:
Crea 2 file necessari per l’analisi nel caso in cui non siano già’ presenti e successivamente esegue del codice
contenuto nelle altre classi del progetto.
I 2 file che crea prima dell’esecuzione delle analisi sono:
1)lastDay.csv: riporta tutti i dischi presenti nel dataset durante l’ultimo giorno a disposizione. Si presume che il
numero dei dischi in ogni giorno sia monotono crescente o al più’ che l’ultimo giorno contenga il (quasi) maggior
numero di record rispetto ad ogni altro giorno.
2)failedDisk.csv: riporta i records dei dischi falliti nell intero dataset. Di ogni disco vengono riportate solo le
informazioni del giorno in cui se ne è’ registrato il fallimento.

‣Utils.java:
Contiene il path in cui risiede il dataset.

‣TrovaSoglie.java
Trova un determinato numero di cluster nelle colonne che secondo noi destano più’ interesse.
Inizia prendendo il file failedDisks.csv e calcola i cluster di ogni colonna in base ai valori in essa contenuti.
Successivamente vengono calcolate le soglie che andranno a stabilire i valori minimi e massimi che verranno usati per
convertire un valore numerico intero e maggiore o uguale in un item. Siccome nella successiva analisi degli itemsets
si e’ notato che, per ogni colonna, solo il primo item risultava frequente (e quindi solo la prima soglia risultava
effettivamente utile) si è’ pensato di ricavare le soglie di ogni colonna eseguendo un clustering del primo cluster.
Quindi, per ogni colonna, si esegue un clustering, si filtrano gli elementi rimuovendo tutti i valori troppo distanti
dal cluster più’ vicino allo 0 e si esegue un nuovo clustering sugli elementi rimanenti.
Il codice rimanente prende il clusters trovati e fornisce in output delle stringhe formattate per essere inserite nel
codice presente in  MiningItemsets.java e AndamentoItemsets.java.

‣MiningItemsets.java
Basandosi sui valori che TrovaSoglie.java restituisce vengono restituiti i frequent itemsets sia usando come dataset
failedDisks.csv (e quindi facendo riferimento ai dischi falliti) sia usando lastDay.csv (e quindi facendo riferimento
ai dischi ancora funzionanti).
Le soglie usate per convertire i valori numerici in items sono create usando TrovaSoglie.java e vanno inseriti a mano
dentro al codice di MiningItemsets.java. Il motivo di questa scelta e’ principalmente per avere un controllo manuale
sulle soglie che vengono usate dal programma: cio’ e’ risultato molto utile in fase di creazione del codice e di debug.

‣AndamentoItemsets.java
Per ogni disco rotto recupera gli ultimi X giorni prima della sua rottura, i record di ogni disco vengono raggruppati
in i-esimi giorni prima della rottura (giorno di rottura, penultimo giorno prima della rottura e così’ via fino al
X-esimo giorno prima della rottura) e compie un’analisi complessiva andando a ricavare l’andamento dei frequent
itemsets.


Varie:
‣ContaDischiFalliti.java
Recupera il numero di dischi falliti in ogni giorno.

‣Statistiche.java
Fornisce delle statistiche per ogni giorno nel dataset.

‣ValoriOgniColonna.java
Conta complessivamente quanti valori non nulli e maggiori di zero contiene ogni colonna. Utile per vedere i valori
S.M.A.R.T. più frequenti nel dataset.