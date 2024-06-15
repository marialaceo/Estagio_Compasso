import hashlib

def main():
    user_input = input("Digite uma string: ")
    
    hash_object = hashlib.sha1(user_input.encode())
    

    print("O hash SHA-1 da string é:", hash_object.hexdigest())

if __name__ == "__main__":
    main()
