from fastapi import FastAPI
from supabase import create_client
from pydantic import BaseModel
from passlib.context import CryptContext


app = FastAPI()
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

SUPABASE_URL = "https://xhtpmskqxuhqnlnfdaqb.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InhodHBtc2txeHVocW5sbmZkYXFiIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc2ODcxODE4MywiZXhwIjoyMDg0Mjk0MTgzfQ.RDNyewkc0GAMuvzr09cNjrPD95fgkOEB4NInHgVaRAo"

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# -----------------------------
# Password Hashing
# -----------------------------
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# -----------------------------
# Request Models
# -----------------------------

class SignUpUser(BaseModel):
    email: str
    password: str
    confirm_password: str
    phone: str


class LoginUser(BaseModel):
    email: str
    password: str


# -----------------------------
# SIGN UP
# -----------------------------

@app.post("/signup")
def signup(user: SignUpUser):

    try:
        print("🔵 Incoming request:", user.dict())

        if user.password != user.confirm_password:
            print("❌ Password mismatch")
            return {"error": "Passwords do not match"}

        print("🔵 Checking existing user...")
        existing = supabase.table("users") \
            .select("id") \
            .eq("email", user.email) \
            .execute()

        print("🟢 Existing user response:", existing.data)

        if existing.data:
            print("❌ User already exists")
            return {"error": "User already exists"}

        print("PASSWORD:", user.password, "LENGTH:", len(user.password))
        print("🔵 Hashing password...")
        hashed_password = pwd_context.hash(user.password)

        print("🔵 Inserting into DB...")
        response = supabase.table("users").insert({
            "email": user.email,
            "password": hashed_password,
            "phone": user.phone
        }).execute()

        print("🟢 Insert response:", response)

        return {"message": "Account created successfully"}

    except Exception as e:
        print("🔥 ERROR OCCURRED:", str(e))
        return {"error": str(e)}
# -----------------------------
# LOGIN
# -----------------------------

@app.post("/login")
def login(user: LoginUser):

    response = supabase.table("users") \
        .select("*") \
        .eq("email", user.email) \
        .execute()

    if not response.data:
        return {"error": "User not found"}

    db_user = response.data[0]

    if not pwd_context.verify(user.password, db_user["password"]):
        return {"error": "Invalid password"}

    return {
        "message": "Login successful",
        "user_id": db_user["id"],
        "email": db_user["email"]
    }


# -----------------------------
# PROFILE
# -----------------------------

@app.get("/profile/{user_id}")
def get_profile(user_id: int):

    response = supabase.table("users") \
        .select("id,email,phone") \
        .eq("id", user_id) \
        .execute()

    return response.data


# -----------------------------
# MARKET PRICES
# -----------------------------

@app.get("/market-prices")
def get_market_prices():

    prices = supabase.table("agmarknet_prices") \
        .select("commodity, state, price, price_date") \
        .order("price_date", desc=True) \
        .limit(20) \
        .execute().data

    return prices


# -----------------------------
# 3 DAY FORECAST
# -----------------------------

@app.get("/commodity-forecast")
def commodity_forecast(commodity: str, state: str):

    price = supabase.table("agmarknet_prices") \
        .select("price, price_date") \
        .eq("commodity", commodity) \
        .eq("state", state) \
        .order("price_date", desc=True) \
        .limit(1) \
        .execute().data

    forecast_rows = supabase.table("agmarknet_price_predictions") \
        .select("predicted_for_date, predicted_price") \
        .eq("commodity", commodity) \
        .eq("state", state) \
        .order("predicted_for_date") \
        .limit(3) \
        .execute().data

    forecast = [
        {
            "date": row["predicted_for_date"],
            "price": row["predicted_price"]
        }
        for row in forecast_rows
    ]

    return {
        "commodity": commodity,
        "state": state,
        "today_price": price[0]["price"] if price else None,
        "forecast": forecast
    }

# -----------------------------
# ROOT
# -----------------------------
@app.get("/")
def home():
    return {"message": "API Running"}