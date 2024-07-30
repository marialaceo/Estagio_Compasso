class Lampada:

  def __init__(self):
    self.estado = False
  

  def liga(self):
    self.estado = True
  

  def desliga(self):
    self.estado = False

  
  def esta_ligada(self):
    return self.estado

lampada = Lampada()
lampada.liga()
print(f'A lâmpada está ligada? {lampada.esta_ligada()}')
lampada.desliga()
print(f'A lâmpada ainda está ligada? {lampada.esta_ligada()}')