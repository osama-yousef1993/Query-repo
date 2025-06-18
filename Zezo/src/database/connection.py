from src.database.database import engine


def execute_all(query_statement):
    with engine.begin() as conn:
        result = conn.execute(query_statement).fetchall()
        if not result:
            return False
        return result


def execute_one(query_statement):
    with engine.begin() as conn:
        result = conn.execute(query_statement).first()
        if not result:
            return False
        return result
