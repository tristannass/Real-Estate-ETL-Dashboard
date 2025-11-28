import pandas as pd
import numpy as np
import random

#  1. EXTRACT (Simulation de données brutes)
def generate_fake_data(n=1000):
    np.random.seed(42)
    data = {
        'id_maison': range(1, n + 1),
        'surface_m2': np.random.normal(90, 30, n).astype(int), # Moyenne 90m2
        'nb_pieces': np.random.randint(1, 6, n),
        'ville': np.random.choice(['Paris', 'Lyon', 'Bordeaux', 'Nantes'], n),
        'date_vente': pd.date_range(start='2023-01-01', periods=n, freq='D'),
        # Prix généré avec une corrélation (surface * prix_m2 + bruit)
        'prix': [] 
    }
    
    # Prix cohérents mais avec du bruit
    base_price = {'Paris': 10000, 'Lyon': 5000, 'Bordeaux': 4500, 'Nantes': 3500}
    for i in range(n):
        ville = data['ville'][i]
        surface = data['surface_m2'][i]
        price = surface * base_price[ville] + np.random.randint(-20000, 20000)
        
        if random.random() < 0.05: # 5% de valeurs manquantes ou aberrantes
            price = np.nan 
        data['prix'].append(price)

    return pd.DataFrame(data)

#  2. TRANSFORM (Nettoyage & Enrichissement)
def process_data(df):
    print(f" Données brutes : {df.shape[0]} lignes")
    
    df_clean = df.dropna(subset=['prix']).copy()
    
    df_clean = df_clean[df_clean['surface_m2'] > 9]
  
    df_clean['prix_m2'] = round(df_clean['prix'] / df_clean['surface_m2'], 2)
    
    # Catégorisation de la surface
    df_clean['categorie_surface'] = df_clean['surface_m2'].apply(
        lambda x: 'Petit' if x < 40 else ('Moyen' if x < 90 else 'Grand')
    )
    
    print(f" Données nettoyées : {df_clean.shape[0]} lignes")
    return df_clean

# 3. LOAD (Export)
if __name__ == "__main__":
    # Exécution du Pipeline
    df_raw = generate_fake_data()
    df_final = process_data(df_raw)
    
    # Export vers CSV 
    df_final.to_csv("immobilier_clean.csv", index=False)
    print("💾 Fichier 'immobilier_clean.csv' généré avec succès !")
