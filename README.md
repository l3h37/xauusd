# Gold Desk Bot (XAUUSD)

Bot Telegram : prix live, analyse M15/H1/H4/D1, niveaux clés, macro (DXY, US10Y...), news,
calendrier éco USD, alertes de prix, mouvement rapide, rappels NFP/CPI/FOMC, calculateur de lot.

## Déploiement gratuit 24h/24 (Render + ping)

1. Telegram → @BotFather → /newbot → copie le token. Utilise un NOUVEAU bot (un token ne peut pas
   servir à la fois à un webhook et à ce bot).
2. Mets ces 4 fichiers dans un dépôt GitHub : main.py, requirements.txt, render.yaml, README.md.
3. Render → New → Blueprint → sélectionne le dépôt (région Frankfurt, plan Free).
   Renseigne TELEGRAM_TOKEN. Laisse CHAT_ID vide pour l'instant.
4. Envoie /start à ton bot : il te donne ton chat id. Ajoute-le dans la variable CHAT_ID sur Render
   (le service redémarre). Le bot est privé : seuls les chat id listés peuvent l'utiliser.
5. Anti-veille : crée un moniteur gratuit sur uptimerobot.com (ou cron-job.org) qui appelle
   https://TON-SERVICE.onrender.com/health toutes les 5 minutes.

## Variables optionnelles
TZ_NAME (Europe/Paris), PRICE_POLL_SECONDS (20), MOVE_ALERT_PCT (0.30), MOVE_WINDOW_MIN (15),
EVENT_LEAD_MIN (15), PUSH_IMPACTS (high), CAL_COUNTRIES (US).

## Limites à connaître
- Render Free : 750 h/mois par workspace. Un seul service allumé en continu = ~744 h. Un 2e service
  toujours réveillé dans le même workspace dépasserait le quota (tous les services free sont alors suspendus).
- Le disque est éphémère : les alertes de prix posées avec /alerte sont perdues à un redémarrage.
- Données gratuites non officielles (Yahoo Finance, repli gold-api.com puis Binance PAXG) :
  pas de bid/ask ni de spread, et le prix peut différer de quelques cents de ton courtier.
- Calendrier : financecalendar.com (attribution affichée dans /cal).
- Outil d'aide à la décision, pas un conseil en investissement.

Test local : TELEGRAM_TOKEN=xxx CHAT_ID=123 python main.py
