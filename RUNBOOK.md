# RUNBOOK

## Test to check if environment works

App ID: spark-4bc269b040a54a9187388212666be3cc
Pod example: test-wordcount-py-786c2c9ddf7882d8-driver
Input: hdfs:///user/ipelekanos/project2026/tmp/test_text.txt
Output: hdfs:///user/ipelekanos/project2026/output/test_wordcount
Result:
('spark', 4)
('data', 2)
('big', 1)
('python', 1)

## Before working on anything

- Connect VPN
- Open WSL
- Load environment:

```bash
cd ~/project2026-bigdata
source ~/bigdata-env.sh
