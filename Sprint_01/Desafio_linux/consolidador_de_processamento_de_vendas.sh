
touch relatorio-final.txt

date +"%Y/%m/%d %H:%M" > relatorio-final.txt

for i in vendas/backup/*.txt; do
    cat $i >> relatorio-final.txt   
done

