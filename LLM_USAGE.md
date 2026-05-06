| Ενότητα | Εργαλείο που χρησιμοποιήθηκε | Έκταση χρήσης | Τι έγραψα  |
|---|---|---|---|
| Ρύθμιση WSL, Docker, Spark, Hadoop, kubectl | ChatGPT | Μερική καθοδήγηση, εξήγηση σφαλμάτων, debugging | Εκτέλεσα ο ίδιος όλες τις εντολές, έλεγξα τα outputs και διόρθωσα το περιβάλλον σύμφωνα με τους οδηγούς του μαθήματος |
| Ρύθμιση OpenVPN, kubeconfig, HDFS και Kubernetes | - | - | Σύνδεσα μόνος μου το VPN, αντέγραψα το kubeconfig, έλεγξα kubectl, DNS και HDFS πρόσβαση |
| spark-defaults.conf και remote test | ChatGPT | Βοήθεια στον εντοπισμό προβλημάτων με spark.kubernetes.file.upload.path και HDFS permissions | Έτρεξα ο ίδιος το remote Spark job, checkαρα driver pod, executor pod, HDFS output και επιβεβαίωσα το _SUCCESS και το part-00000 |
| README.md | - | - | Επεξεργάστηκα τα δικά μου στοιχεία, project, ΑΜ, namespace και HDFS paths |
| RUNBOOK.md | - | - | Ενημερώνεται από εμένα με τις πραγματικές εντολές, application IDs, HDFS paths και αποτελέσματα κάθε job |
| project2026/jobs/common.py | ChatGPT | Αρχικός σκελετός helper module, HDFS helpers, metrics helpers | Έλεγξα τη χρήση του στο remote Spark submit, διόρθωσα τη ροή με `--py-files και το χρησιμοποιώ ως κοινό module για τα jobs |
| Parquet preparation | ChatGPT | Αρχική υλοποίηση για normalization και debugging | Έτρεξα το job στην απομακρυσμένη υποδομή, επιβεβαίωσα HDFS Parquet outputs, partitioning, metrics JSON, Spark application ID |
| EDA | ChatGPT | Βοήθησε στον σκελετό του EDA Spark job, στην επιλογή μετρικών και στο plotting workflow | Εκτέλεσα το Spark job στο Kubernetes, έλεγξα τα HDFS outputs, επιβεβαίωσα το eda_metrics.json, κατέβασα τα tables, εγκατέστησα τις τοπικές βιβλιοθήκες, παρήγαγα τα PNG plots |
| Q1 - Demand analysis | - | - |
| Q2 - Speed/congestion analysis | - | - |
| Q3 - Revenue/zones analysis | - | - |
| Q4 - Payments/tips analysis | - | - |
| Q5 - Airport/borough flows | - | - |
| Q6 - Pickup/dropoff imbalance | - | - |
