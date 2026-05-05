"""
security.py
-----------
Sécurisation des fichiers sensibles.

Responsabilités :
  - Vérifier que le fichier de connexion n'est lisible que par son propriétaire
  - Appliquer chmod 600 sur demande (Linux/macOS)
  - Émettre des avertissements appropriés sous Windows
"""

import logging
import os
import stat
from pathlib import Path

log = logging.getLogger(__name__)


def check_permissions(path: Path) -> bool:
    """
    Vérifie que le fichier est sécurisé (lisible uniquement par son propriétaire).

    Retourne True si les permissions sont correctes, False sinon.
    Émet un avertissement si les permissions sont trop ouvertes.
    """
    if os.name == "nt":
        log.warning(
            "⚠️  Windows détecté : vérifiez manuellement que '%s' "
            "n'est accessible qu'à votre compte utilisateur.", path.name
        )
        return True  # On ne peut pas vérifier finement sous Windows

    mode = stat.S_IMODE(path.stat().st_mode)
    if mode & (stat.S_IRWXG | stat.S_IRWXO):
        log.warning(
            "⚠️  RISQUE DE SÉCURITÉ : '%s' est lisible par le groupe ou "
            "d'autres utilisateurs (mode %o).\n"
            "    Corrigez avec : chmod 600 %s",
            path.name, mode, path
        )
        return False

    log.info("Permissions '%s' : OK (mode %o)", path.name, mode)
    return True


def apply_secure_permissions(path: Path) -> None:
    """
    Applique chmod 600 sur le fichier (Linux/macOS uniquement).
    Le fichier ne sera alors lisible que par son propriétaire.
    """
    if os.name == "nt":
        log.warning("chmod non disponible sous Windows — action ignorée.")
        return

    try:
        path.chmod(0o600)
        log.info("✅  chmod 600 appliqué sur '%s'", path)
    except PermissionError:
        log.error("❌  Permission refusée pour modifier '%s' — lancez en tant que propriétaire.", path)
    except Exception as e:
        log.error("❌  Impossible d'appliquer chmod 600 sur '%s' : %s", path, e)
