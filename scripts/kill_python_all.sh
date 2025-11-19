#!/usr/bin/env bash

# Script pour tuer tous les processus Python sur toutes les machines listées dans config/nodes.txt

# Chemin vers le fichier de configuration des nœuds
NODES_FILE="config/nodes.txt"
USERNAME="blepourt-25"

# Vérifier que le fichier nodes.txt existe
if [[ ! -f "$NODES_FILE" ]]; then
    echo "Erreur: Le fichier $NODES_FILE n'existe pas"
    exit 1
fi

# Lire le fichier ligne par ligne et exécuter la commande pkill sur chaque machine
echo "Arrêt des processus Python sur toutes les machines..."
while IFS= read -r node; do
    # Ignorer les lignes vides
    if [[ -z "$node" ]]; then
        continue
    fi
    
    echo "→ Arrêt des processus Python sur $node..."
    ssh -n -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "$USERNAME@$node" "pkill -f python" 2>/dev/null
    exit_code=$?
    
    # Vérifier le code de retour
    if [[ $exit_code -eq 0 ]] || [[ $exit_code -eq 1 ]]; then
        echo "  ✓ Commande exécutée sur $node"
    else
        echo "  ✗ Erreur de connexion sur $node"
    fi
done < "$NODES_FILE"

echo "Terminé!"
