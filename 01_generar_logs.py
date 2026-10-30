import json
import random
import uuid
from datetime import datetime, timedelta


def generar_logs_saas(num_registros=50000):
    eventos = ["login", "view_dashboard", "export_data", "checkout", "error"]
    sistemas = ["macOS", "Windows", "iOS", "Android"]

    with open("raw_events.json", "w") as f:
        for _ in range(num_registros):
            evento = {
                "event_id": str(uuid.uuid4()),
                "timestamp": (
                    datetime.now() - timedelta(minutes=random.randint(0, 10000))
                ).isoformat(),
                "user": {
                    "user_id": f"USR-{random.randint(100, 999)}",
                    "plan": random.choices(
                        ["free", "pro", "enterprise", None], weights=[50, 30, 15, 5]
                    )[0],
                },
                "device": {
                    "os": random.choice(sistemas),
                    "browser": random.choice(["Chrome", "Safari", "Firefox"]),
                },
                "action": random.choice(eventos),
                "metadata": [
                    {"key": "load_time_ms", "value": random.randint(100, 2000)}
                ]
                if random.random() > 0.5
                else [],
            }
            f.write(json.dumps(evento) + "\n")

    print(f"Generados {num_registros} eventos crudos en 'raw_events.json'")


if __name__ == "__main__":
    generar_logs_saas()
