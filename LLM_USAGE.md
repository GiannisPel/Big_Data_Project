| Ενότητα | Εργαλείο που χρησιμοποιήθηκε | Έκταση χρήσης | Τι έγραψα  |
|---|---|---|---|
| Ρύθμιση WSL, Docker, Spark, Hadoop, kubectl | ChatGPT | Μερική καθοδήγηση, εξήγηση σφαλμάτων, debugging | Εκτέλεσα ο ίδιος όλες τις εντολές, έλεγξα τα outputs και διόρθωσα το περιβάλλον σύμφωνα με τους οδηγούς του μαθήματος |
| Ρύθμιση OpenVPN, kubeconfig, HDFS και Kubernetes | - | - | Σύνδεσα μόνος μου το VPN, αντέγραψα το kubeconfig, έλεγξα kubectl, DNS και HDFS πρόσβαση |
| spark-defaults.conf και remote test | ChatGPT | Βοήθεια στον εντοπισμό προβλημάτων με spark.kubernetes.file.upload.path και HDFS permissions | Έτρεξα ο ίδιος το remote Spark job, checkαρα driver pod, executor pod, HDFS output και επιβεβαίωσα το _SUCCESS |
| Parquet preparation | ChatGPT | Αρχική υλοποίηση για normalization και debugging | Έτρεξα το job στην απομακρυσμένη υποδομή, επιβεβαίωσα HDFS Parquet outputs, partitioning, metrics JSON, Spark application ID |
| EDA | ChatGPT | Βοήθησε στον σκελετό του EDA Spark job, στην επιλογή μετρικών και στο plotting workflow | Εκτέλεσα το Spark job στο Kubernetes, έλεγξα τα HDFS outputs, επιβεβαίωσα το eda_metrics.json, κατέβασα τα tables, εγκατέστησα τις τοπικές βιβλιοθήκες, παρήγαγα τα PNG plots |
| Q1 | ChatGPT | Βοήθησε ως σύμβουλος δομής των scripts, των output paths και την δομή των metrics και debugging | Η εκτέλεση, ο έλεγχος και η αξιολόγηση έγιναν από εμένα: έτρεξα τα jobs στο cluster, εντόπισα και επαλήθευσα τα παραγόμενα HDFS αρχεία, έλεγξα ότι τα RDD/DataFrame/SQL αποτελέσματα συμφωνούν, κράτησα screenshot και θα γράψω ο ίδιος τη σύγκριση χρόνων, plans και συμπερασμάτων στην αναφορά |
| Q2 | ChatGPT | Βοήθησε στη δόμη των Helpers, διόρθωση των μαθηματικών συναρτήσεων και debugging| εκτέλεσα τις υλοποιήσεις στο Spark/Kubernetes, έλεγξα τα HDFS outputs, σύγκρινα χρόνους και αποτελέσματα, κράτησα screenshots. |
| Q3 | ChatGPT | Βοήθεια στο πως πρέπει να είναι δομημένο και debugging | έτρεξα τις υλοποιήσεις στο Spark/Kubernetes, έλεγξα τα HDFS outputs, συνέκρινα τα result tables και τους χρόνους, επιβεβαίωσα τα physical plans, δημιούργησα τα plots τοπικά |
| Q4 | ChatGPT | Βοήθησε στη κατανόηση της δομής, στη σύγκριση CSV/Parquet | Εγώ υλοποίησα έτρεξα τις υλοποιήσεις στο Spark/Kubernetes, έλεγξα τα HDFS outputs, σύγκρινα τα metrics και τους χρόνους, κράτησα τα Spark application IDs/plans, δημιούργησα τα plots |
| Q5 | - | - | - |
| Q6 | - | - | - |
