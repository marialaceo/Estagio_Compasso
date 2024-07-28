mkdir vendas
cp ecommerce/* vendas
mkdir vendas/backup
cp vendas/dados_de_vendas.csv vendas/backup
mv vendas/backup/dados_de_vendas.csv vendas/backup/backup-dados-$(date +"%Y%m%d").csv
touch vendas/backup/relatorio$(date +"%Y%m%d-%H%M%S").txt

date +"%Y/%m/%d %H:%M" > vendas/backup/relatorio$(date +"%Y%m%d-%H%M%S").txt
cat vendas/backup/backup-dados-$(date +"%Y%m%d").csv | cut -d ',' -f 5 | sed -n '2p' >> vendas/backup/relatorio$(date +"%Y%m%d-%H%M%S").txt
cat vendas/backup/backup-dados-2$(date +"%Y%m%d").csv | cut -d ',' -f 5 | sed -n '67p' >> vendas/backup/relatorio$(date +"%Y%m%d-%H%M%S").txt
cat vendas/backup/backup-dados-$(date +"%Y%m%d").csv | awk -F ',' 'NR > 1 { soma+=$3 } END { print soma }' >> vendas/backup/relatorio$(date +"%Y%m%d-%H%M%S").txt
head -n 10 vendas/backup/backup-dados-$(date +"%Y%m%d").csv >> vendas/backup/relatorio$(date +"%Y%m%d-%H%M%S").txt

zip vendas/backup/backup-dados-$(date +"%Y%m%d").zip vendas/backup/backup-dados-$(date +"%Y%m%d").csv
rm vendas/backup/backup-dados-$(date +"%Y%m%d").csv

