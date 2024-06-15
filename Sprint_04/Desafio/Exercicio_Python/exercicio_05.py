import csv

with open('estudantes.csv', 'r') as entrada:
  leitor = csv.reader(entrada)
  estudantes = list(leitor)

estudantes_ordenados = sorted(estudantes, key=lambda estudante: estudante[0])
resultados = list(map(lambda estudante: 
                      f"Nome: {estudante[0]} Notas: {sorted(map(float, estudante[1:]), reverse=True)[:3]} Média: {round(sum(sorted(map(float, estudante[1:]), reverse=True)[:3]) / 3, 2)}",
                      estudantes_ordenados))

for resultado in resultados:
    print(resultado)