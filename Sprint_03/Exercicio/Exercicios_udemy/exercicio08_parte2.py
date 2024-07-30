a = ['maça', 'arara', 'audio', 'radio', 'radar', 'moto']

for i in a:
    i_invertido = i[::-1]
    if i == i_invertido:
        print(f'A palavra: {i} é um políndromo')
        print()
    else:
        print(f'A palavra: {i} não é um políndromo')
        print()
    

