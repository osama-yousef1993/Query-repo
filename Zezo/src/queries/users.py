from sqlalchemy.exc import NoResultFound

from src.database.connection import execute_one
from src.models.user import users


class UserQuery:
    def authenticate_user(self, email: str, password: str) -> bool:
        try:
            result = users.select().where(users.c.email == email)
            row = execute_one(result)
            if not row:
                return False
            if row.password == password:
                return True
            return False
        except NoResultFound:
            return False
