import re
import pandas as pd
from dagster import job, op

NOMBRE_CSV = 'Anime_data.csv'

@op
def extract():
    df_animes = pd.read_csv(NOMBRE_CSV, sep = ',')

    return df_animes

@op
def transform(dataframe):
    df_animes_reducido = dataframe[['Anime_id','Title', 'Genre', 'Synopsis', 'Episodes', 'Link']]
    df_animes_reducido = df_animes_reducido[df_animes_reducido['Genre'].notna()]
    df_animes_final = df_animes_reducido
    df_animes_final['Genre'] = df_animes_reducido['Genre'].apply(generos_tolist)

    generos_disponibles = get_genres(df_animes_reducido)
    return (df_animes_final, generos_disponibles)

@op
def load(tupla):

    dataframe = tupla[0]
    generos_disponibles = tupla[1]
    print('Géneros disponibles')

    tmp = generos_disponibles.copy()
    generos = str(tmp.pop(0))

    for genero in tmp:
        generos += ', ' + genero
    print(generos)
    recomendacion = dataframe.sample()
    imprimir_anime(recomendacion)

    return 
'''    genero = input('Inserte el género que desee: ').lower()

    genero_invalido = True

    while genero_invalido:

        df_animes_recomendar = dataframe.copy()
        df_animes_recomendar['Genre'] = dataframe['Genre'].apply(buscar_genero, args = [genero])
        df_animes_recomendar = df_animes_recomendar[df_animes_recomendar['Genre'].notna()]

        if len(df_animes_recomendar) != 0:
            genero_invalido = False
        else:
            genero = input('Género inválido, intoduzca uno de la lista: ').lower()
    
    anime = input('Si lo desea, puede introducir un anime de ese género para mejorar nuestra recomendación: ')
    
    if anime:
    
        df_anime = df_animes_recomendar.copy()
        df_anime['Title'] = df_animes_recomendar['Title'].apply(animes_similares, args = [anime])
        df_anime = df_anime[df_anime['Title'].notna()]

        if len(df_anime) != 0:
            for anime in df_anime['Title']:
                print(anime)
            anime = input('¿Cual de estos es el anime que has visto? (inserte nombre completo): ')
            
            if anime:
                df_anime['Title'] = df_animes_recomendar['Title'].apply(anime_identico, args = [anime])
                df_anime = df_anime[df_anime['Title'].notna()]

                if len(df_anime) == 1:
                    existe_anime = re.match('\{\d+:', str(df_anime['Title'].to_dict()))
                    num_anime = int(re.sub('[\{\:]','', str(existe_anime.group())))
                    recomendacion = buscar_similar(df_animes_recomendar, num_anime)

            else:
                print('Lo sentimos, el nombre del anime no coincide con ninguno de nuestra base de datos.')
                print('Por ello, le recomendamos otro anime del mismo género')
                recomendacion = df_animes_recomendar.sample()

        else:

            print('Lo sentimos, el nombre del anime no coincide con ninguno de nuestra base de datos.')
            print('Por ello, le recomendamos otro anime del mismo género')
            recomendacion = df_animes_recomendar.sample()

    else:

        recomendacion = df_animes_recomendar.sample()
        '''


def generos_tolist(generos): #Se recibe un string que en realidad tiene estructura de lista
    generos = re.sub('[\\[\\]\'\s]', '', generos)
    
    generos = re.split(',', generos.lower())

    return generos

def get_genres(df_animes):

    all_generos = df_animes['Genre']

    lista_generos = []

    for generos in all_generos:

        for genero in generos:

            if genero not in lista_generos:

                lista_generos.append(genero)

    return lista_generos

def buscar_genero(generos, genero):
    for genero2 in generos:

        if genero == genero2:

            return generos

    return float('nan')

def animes_similares(anime_name, anime):
    existe_anime = re.match(anime.lower(), anime_name.lower())
    if existe_anime:
        return anime_name
    else:
        return float('nan')

def anime_identico(anime_name, anime):
    if anime_name.lower() == anime.lower():
        return anime_name
    else:
        return float('nan')

def imprimir_anime(anime):
    print('Creemos que el siguiente anime podría gustarle')
    anime = anime.to_dict()
    tmp_anime = re.match('\\{\d+\\:', str(anime['Title']))
    if tmp_anime:
        num_anime = int(re.sub('[{:]','',tmp_anime.group()))

        print('\n')

        print(f"Nombre: {anime['Title'][num_anime]}")

        print('\n')
        
        print(f"Géneros: {anime['Genre'][num_anime]}")

        print('\n')
        
        if anime['Synopsis'][num_anime] != float('nan'):
            print(f"Synopsis: {anime['Synopsis'][num_anime]}")
        else:
            print("No dispone de synopsis")
        
        print('\n')

        try:
            print(f"Nº episodios: {int(anime['Episodes'][num_anime])}")
        except:
            print("Episodios por confirmar")

        print('\n')
        
        if anime['Link'][num_anime] != float('nan'):
            print(f"Link para ver la serie: {anime['Link'][num_anime]}")
        else:
            print('Lamentablemente, no disponemos de link para ver la serie')
        
        print('\n')
        
    else:

        print(f"Nombre: {anime['Title']}")

        print('\n')
        
        print(f"Géneros: {anime['Genre']}")

        print('\n')
        
        if anime['Synopsis'] != float('nan'):
            print(f"Synopsis: {anime['Synopsis']}")
        else:
            print("No dispone de synopsis")
            
        print('\n')
        
        try:
            print(f"Nº episodios: {int(anime['Episodes'])}")
        except ValueError:
            print("Episodios por confirmar")

        print('\n')
        
        if anime['Link'] != float('nan'):
            print(f"Link para ver la serie: {anime['Link']}")
        else:
            print('Lamentablemente, no disponemos de link para ver la serie')
        
        print('\n')
        
    return 

def buscar_similar(dataframe, numero):
    numero += 1
    no_anime = True
    while no_anime:
        try:

            recomendado = dataframe.loc[numero]
            no_anime = False
            return recomendado
            
        except KeyError:
            numero += 1

        except IndexError:
            numero = 0

@job
def main():
    load(transform(extract()))
    return 
'''    df_animes, generos = transform(extract())
    seguir = False
    while seguir:
        load(df_animes, generos)
        continuar = input('¿Quiere alguna otra recomendación? (y/n): ')

        if continuar.lower() != 'y':
            seguir = False
    print('Gracias por confiar en nosotros')
'''
    
