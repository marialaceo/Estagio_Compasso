from random import randint

class Pessoa:

  def __init__(self):
    self.id = randint(1,10)
  

  def set_nome(self, nome):
    self._nome = nome

  
  def get_nome(self):
    return self._nome
  

pessoa = Pessoa()
pessoa.set_nome('Maria Beatriz')
print(pessoa.get_nome())