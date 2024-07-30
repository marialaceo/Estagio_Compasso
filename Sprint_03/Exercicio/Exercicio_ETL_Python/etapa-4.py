from collections import defaultdict

contador_de_filmes = defaultdict(int)

with open('actors.csv', 'r') as entrada:
  linhas = entrada.readlines()
  cabecalho = linhas[0]

  for linha in linhas[1:]:
    colunas = linha.strip().split(',')

    if len(colunas) < 6:
      continue

    filmes = colunas[4].strip()
    contador_de_filmes[filmes] += 1

lista_de_filmes = sorted(contador_de_filmes.items())

sentencas = []
for i, (filme, contador )in enumerate(lista_de_filmes, start=1):
  sentencas.append(f'{i} - {filme} aparece {contador}')


with open('etapa-4.txt', 'w') as saida:
  for sentenca in sentencas:
    saida.write(sentenca + '\n')

for sentenca in sentencas:
    print(sentenca)
