class Ordenadora:
  
  def ordenacaoCrescente(self, list ):
    nova_lista = sorted(list)
    return nova_lista
  
  def ordenacaoDecrescente(self, list ):
    nova_lista = sorted(list, reverse=True)
    return nova_lista 


lista1 = Ordenadora()
print(lista1.ordenacaoCrescente([3,4,2,1,5]))

lista2 = Ordenadora()
print(lista2.ordenacaoDecrescente([9,7,6,8]))