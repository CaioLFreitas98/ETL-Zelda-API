import requests
import os
import json
from time import sleep

BASE_URL = "https://zelda.fanapis.com/api/"

def fetch_all(endpoint: str, delay: float = 0.5):
    """
    Faz paginação automática de /api/{endpoint}?page=1,2,...
    Combina todos os itens em uma lista e salva num único arquivo JSON.
    """
    page = 1
    all_data = []
    
    while True:
        url = f"{BASE_URL}{endpoint}?page={page}"
        print(f"Solicitando: {url}")
        r = requests.get(url)
        if r.status_code != 200:
            print(f"  ❌ Erro {r.status_code} em {url}")
            break
        
        payload = r.json()
        items = payload.get("data") or payload.get("results") or []
        if not items:
            print("  ▶️  Sem mais dados, finalizando paginação.")
            break
        
        all_data.extend(items)
        print(f"  ✅ Página {page} – retornou {len(items)} itens.")
        page += 1
        sleep(delay)

    os.makedirs("data/raw", exist_ok=True)
    out_path = f"data/raw/{endpoint}.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({"data": all_data}, f, indent=2, ensure_ascii=False)
    print(f"► Todos os {len(all_data)} itens de '{endpoint}' salvos em {out_path}")


if __name__ == "__main__":
    endpoints = [
        "games",
        "characters",
        "monsters",
        "dungeons"
    ]
    
    for ep in endpoints:
        fetch_all(ep)
