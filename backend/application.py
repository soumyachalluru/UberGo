from flask import Flask, request, jsonify
from flaskext.mysql import MySQL
import random
from flask_cors import CORS
from datetime import date,datetime
import string
import pandas as pd
import numpy as np
import mysql.connector
import os
from decimal import Decimal
from mysql.connector import Error
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
from lightgbm import LGBMRegressor
from pandas.tseries.holiday import USFederalHolidayCalendar
from redis import StrictRedis

cal = USFederalHolidayCalendar()

application = Flask(__name__)
CORS(application)

# Redis configuration
redis_cache = StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)

def generate_unique_string():
    # Combine uppercase letters, lowercase letters, and digits
    characters = string.ascii_uppercase + string.ascii_lowercase + string.digits
    
    # Randomly sample 26 unique characters
    unique_string = ''.join(random.sample(characters, 26))
    return unique_string

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME", "ubergo_db")
}

# Function to create a database connection
def create_connection():
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        if connection.is_connected():
            return connection
    except Error as e:
        print("Error in create connection")
        print(f"Error: {e}")
        return None

# # check session
# def checkusersession(uuid):
#     try:
#         conn = create_connection()
#         cursor = conn.cursor()
#         cursor.execute("SELECT status FROM ods  WHERE uuid =%s", (uuid,))
#         user = cursor.fetchone()
#         cursor.close()
#         conn.close()
#         if user:
#             if user[0] == 'verified':
#                 return True
#             else:
#                 return False
#         else:
#             return False

#     except Exception as e:
#         return False

# checking for usersession with caching
def checkusersession(uuid):
    try:
        # Check Redis cache first
        cached_status = redis_cache.get(f"session:{uuid}")
        if cached_status:
            return cached_status == 'verified'

        # Fallback to database query
        conn = create_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT status FROM ods WHERE uuid = %s", (uuid,))
        user = cursor.fetchone()
        cursor.close()
        conn.close()

        if user:
            status = user[0]
            # Cache the result in Redis for 5 min
            redis_cache.setex(f"session:{uuid}", 300, status)
            return status == 'verified'
        else:
            return False
    except Exception as e:
        print(f"Error in checkusersession: {e}")
        return False

# def getuseridbyuuid(uuid):
#     try:
#         conn = create_connection()
#         cursor = conn.cursor()
#         cursor.execute("SELECT user_id FROM `ods` WHERE uuid =%s", (uuid,))
#         user = cursor.fetchone()
#         cursor.close()
#         conn.close()
#         if user:
#            return user[0]
#         else:
#             return False

#     except Exception as e:
#         return False

# Function to get user ID by UUID (with caching)
def getuseridbyuuid(uuid):
    try:
        # Check Redis cache first
        cached_userid = redis_cache.get(f"user_id:{uuid}")
        if cached_userid:
            return cached_userid

        # Fallback to database query
        conn = create_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT user_id FROM ods WHERE uuid = %s", (uuid,))
        user = cursor.fetchone()
        cursor.close()
        conn.close()

        if user:
            user_id = user[0]
            # Cache the user ID in Redis for 5 min
            redis_cache.setex(f"user_id:{uuid}", 300, user_id)
            return user_id
        else:
            return None
    except Exception as e:
        print(f"Error in getuseridbyuuid: {e}")
        return None

@application.route('/test_connection')
def test_connection():
    try:
        # Establish the connection
        conn =  create_connection()
        cursor = conn.cursor()

        # Optional: Execute a simple query
        cursor.execute("SELECT DATABASE();")  # Get the current database
        current_db = cursor.fetchone()

        # Clean up
        cursor.close()
        conn.close()

        return jsonify({'status': 'Success', 'message': f'Connected to database {current_db[0]}'}), 200
    except Exception as e:
        return jsonify({'status': 'Failed', 'message': str(e)}), 500


# Route for user registration
@application.route('/register', methods=['POST'])
def register():
    try:
        # Get data from request
        data = request.get_json()
        username = data.get('full_name')
        emailid = data.get('emailid')
        mobile = data.get('mobile')
        gender = data.get('gender')
        password = data.get('password')
        driverstatus = data.get('driverstatus')
        address = data.get('address')
        city = data.get('city')
        state = data.get('state')
        zip = data.get('zip')

        if not username or not password:
            return jsonify({'status':'Failed','message': 'Missing Perameters'}), 400

        userid = 'CSH' + str(random.randint(10000, 99999))
        # Create a cursor and insert user into the database
        conn = create_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT email_id FROM users WHERE email_id=%s OR mobile_no =%s",(emailid,mobile))
        check = cursor.fetchone()
        if not check:
            cursor.execute("INSERT INTO `users`(`full_name`, `email_id`, `user_id`, `mobile_no`, `gender`, `blocking_status`, `wallet_balance`, `date_of_joining`, `password`, `driver_status`, `address`, `city`, `state`, `zip_code`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", 
        (username, emailid,userid,mobile,gender,'0','0',date.today(),password,driverstatus,address,city,state,zip))
            conn.commit()

            cursor.close()
            conn.close()
        else:
            return jsonify({'status':'Failed','message': 'User Already Registered'}), 400
        
        return jsonify({'status':'Success','message': 'User registered successfully!'}), 201

    except Exception as e:
        return jsonify({'status':'Failed','message': 'We Are Facing Some Technical Issue Please try Again'+str(e)}), 500

# # Route for user login
# @application.route('/login', methods=['POST'])
# def login():
#     try:
#         # Get data from request
#         data = request.get_json()
#         emailorphone = data.get('emailid')
#         password = data.get('password')
#         randomtextfokey = generate_unique_string()

#         if not emailorphone or not password:
#             return jsonify({'status': 'Failed', 'message': 'User Credential are required!'}), 400

#         # Generate a One-Time-Secret (OTS)
#         ots = str(random.randint(10000, 99999))

#         # Database connection
#         conn = create_connection()
#         cursor = conn.cursor()
#         # Authenticate user and check blocking status before
#         cursor.execute(
#             "SELECT user_id, mobile_no,driver_status FROM `users` WHERE (email_id = %s OR mobile_no = %s) AND password = %s AND blocking_status ='0'",
#             (emailorphone, emailorphone, password)
#         )
#         user = cursor.fetchone()

#         if not user:
#             return jsonify({'status': 'Failed', 'message': 'Invalid Credential!'}), 401

#         user_id, mobile_no,driver_status = user

#         # Fix: Ensure user_id is passed as a tuple
#         cursor.execute("SELECT ots FROM `ods` WHERE user_id = %s AND status = 'verified'", (user_id,))
#         check = cursor.fetchone()

#         if not check:
#             cursor.execute(
#                 "INSERT INTO `ods`(`mobile_no`, `ots`, `user_id`, `uuid`, `status`, `dates`) VALUES (%s, %s, %s, %s, %s, %s)",
#                 (mobile_no, ots, user_id, randomtextfokey, 'verified', date.today())
#             )
#         else:
#             cursor.execute("UPDATE `ods` SET `status`='unverified' WHERE user_id = %s", (user_id,))
#             cursor.execute(
#                 "INSERT INTO `ods`(`mobile_no`, `ots`, `user_id`, `uuid`, `status`, `dates`) VALUES (%s, %s, %s, %s, %s, %s)",
#                 (mobile_no, ots, user_id, randomtextfokey, 'verified', date.today())
#             )

#         # Commit changes
#         conn.commit()

#         # Close resources
#         cursor.close()
#         conn.close()

#         # Return success response with UUID
#         return jsonify({'status': 'Success', 'message': 'Login successful!', 'uuid': randomtextfokey,"driver_status":user[2]}), 200

#     except mysql.connector.Error as e:
#         # Handle MySQL-specific errors
#         return jsonify({'status': 'Failed', 'message': "We Are Facing Some Technical Issue Please try Again DB"}), 500

#     except Exception as e:
#         # Handle general exceptions
#         return jsonify({'status': 'Failed', 'message': 'We Are Facing Some Technical Issue Please try Again'+str(e)}), 50

#Route for user login (caching UUID upon login)
@application.route('/login', methods=['POST'])
def login():
    try:
        # Get data from request
        data = request.get_json()
        emailorphone = data.get('emailid')
        password = data.get('password')
        randomtextfokey = generate_unique_string()

        if not emailorphone or not password:
            return jsonify({'status': 'Failed', 'message': 'User Credential are required!'}), 400

        # Database connection
        conn = create_connection()
        cursor = conn.cursor()
        # Authenticate user
        cursor.execute(
            "SELECT user_id, mobile_no, driver_status FROM users WHERE (email_id = %s OR mobile_no = %s) AND password = %s AND blocking_status = '0'",
            (emailorphone, emailorphone, password)
        )
        user = cursor.fetchone()

        if not user:
            return jsonify({'status': 'Failed', 'message': 'Invalid Credential!'}), 401

        user_id, mobile_no, driver_status = user

        # Cache the session in Redis
        redis_cache.setex(f"session:{randomtextfokey}", 300, "verified")
        redis_cache.setex(f"user_id:{randomtextfokey}", 300, user_id)
        
        # Generate a One-Time-Secret (OTS)
        ots = str(random.randint(10000, 99999))

        # Store the session in the database
        cursor.execute(
            "INSERT INTO `ods`(`mobile_no`, `ots`, `user_id`, `uuid`, `status`, `dates`) VALUES (%s, %s, %s, %s, %s, %s) "
            "ON DUPLICATE KEY UPDATE status = 'verified', uuid = %s",
            (mobile_no, ots, user_id, randomtextfokey, 'verified', date.today(), randomtextfokey)
        )
        conn.commit()

        cursor.close()
        conn.close()

        return jsonify({'status': 'Success', 'message': 'Login successful!', 'uuid': randomtextfokey, 'driver_status': driver_status}), 200
    except Exception as e:
        return jsonify({'status': 'Failed', 'message': f'We Are Facing Some Technical Issue: {e}'}), 500
    finally:
        # Ensure all resources are closed properly
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

# Route for user logout
@application.route('/logout', methods=['POST'])
def logout():
    try:
        # Get data from request
        data = request.get_json()
        uuid = data.get('uuid')

        if not uuid:
            return jsonify({'status': 'Failed', 'message': 'Invalid session Id'}), 400

        # Create a connection and cursor
        conn = create_connection()
        cursor = conn.cursor()
        # Update the user's status to unverified
        cursor.execute("UPDATE `ods` SET `status`='unverified' WHERE uuid = %s", (uuid,))
        conn.commit()
        cursor.close()
        conn.close()
        if cursor.rowcount == 0:
            # No rows were updated, meaning the uuid was invalid
            return jsonify({'status': 'Failed', 'message': 'Invalid Seesion Try Again'}), 401

        return jsonify({'status': 'Success', 'message': 'Logout successful'}), 200

    except Exception as e:
        return jsonify({'status': 'Failed', 'message': 'We Are Facing Some Technical Issue Please try Again'+str(e)}), 500

# Route for get profile
@application.route('/getprofile', methods=['POST'])
def getprofile():
    try:
        # Get data from request
        data = request.get_json()
        uuid = data.get('uuid')

        if not uuid:
            return jsonify({'status':'Failed','message': 'Session Missmatched'}), 400

        # Create a cursor and check the user in the database
        conn = create_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT u.* FROM users u JOIN ods o ON u.user_id = o.user_id WHERE o.uuid =%s AND o.status = 'verified'", (uuid,))
        user = cursor.fetchone()

        cursor.close()
        conn.close()

        if user:
            # Return the user ID if login is successful
            return jsonify({'status':'Success','message': 'Fetch Success','user_id': user[3], 'full_name': user[1],'email_id':user[2],'mobile_no':user[4],'gender':user[5],'wallet_balance':user[7],'date':user[8],'driver_status':user[10],'address':user[11],'city':user[12],'state':user[13],'zip':user[14]}), 200
        else:
            return jsonify({'status':'Failed','message': 'Session Missmatched'}), 401

    except Exception as e:
        return jsonify({'message': 'We Are Facing Some Technical Issue Please try Again'+str(e)}), 500

# Route for change password
@application.route('/change_password', methods=['POST'])
def changepassword():
    try:
        # Get data from request
        data = request.get_json()
        uuid = data.get('uuid')
        oldpass = data.get('oldpass','')
        newpass = data.get('newpass','')
        userid = getuseridbyuuid(uuid)
        if checkusersession(uuid):
            if not uuid or not oldpass:
             return jsonify({'status':'Failed','message': 'Details are required!'}), 400


            # Create a cursor and insert user into the database
            conn = create_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT password FROM users WHERE user_id=%s",(userid,))
            pas = cursor.fetchone()
            print(pas)
            if pas[0] == oldpass:
                cursor.execute("UPDATE users SET password=%s WHERE user_id=%s", (newpass,userid))
                conn.commit()

                cursor.close()
                conn.close()
            else:
                return jsonify({'status':'Failed','message': 'Invalid Password'}), 400
            
            return jsonify({'status':'Success','message': 'Password Change Success'}), 201
        else:
            return jsonify({'status':'Failed','message': 'Invalid session'}), 401
        
    except Exception as e:
        return jsonify({'status':'Failed','message': 'We Are Facing Some Technical Issue Please try Again '+str(e)}), 500

# Route for checksession    
@application.route('/checksession', methods=['POST'])
def checksession():
    try:
        # Get data from request
        data = request.get_json()
        uuid = data.get('uuid')

        if not uuid:
            return jsonify({'status':'Failed','message': 'Session Missmatched'}), 400

        # Create a cursor and check the user in the database
        conn = create_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT status FROM ods  WHERE uuid =%s", (uuid,))
        user = cursor.fetchone()

        cursor.close()
        conn.close()

        if user:
            # Return the user ID if login is successful
            return jsonify({'status':'Success','message': 'Fetch Success', 'session_status': user[0]}), 200
        else:
            return jsonify({'status':'Failed','message': 'We Are Facing Some Technical Issue Please try Again'}), 401

    except Exception as e:
        return jsonify({'message': 'We Are Facing Some Technical Issue Please try Again'+str(e)}), 500

# # Route for get travel history
# @application.route('/gettravelhistory', methods=['POST'])
# def gettravelhistory():
#     try:
#             # Get data from request
#             data = request.get_json()
#             uuid = data.get('uuid')

#             if not uuid:
#                 return jsonify({'status':'Failed','message': 'Session Id Invalid'}), 400

#             # Create a cursor and check the user in the database
#             if checkusersession(uuid):   
#                 conn = create_connection()
#                 cursor = conn.cursor()
#                 cursor.execute("SELECT u.* FROM travel_history u JOIN ods o ON u.customer_id = o.user_id WHERE o.uuid = %s AND o.status = 'verified' and u.riding_status !='4'", (uuid,))
#                 user = cursor.fetchone()

#                 cursor.close()
#                 conn.close()

#                 if user:
#                     return jsonify({'status':'Success','message': 'Fetch Success','id':user[0],'user_id': user[1],'Customer_name':user[2],'driverid':user[3],'pickup_location':user[4],
#                             'drop_location':user[5],'pickup_time':user[6],'drop_time':user[7],'amount':user[8],'date':user[9],'riding_status':user[10],'otp':user[11],
#                             'pickup_lati':user[12],'drop_lati':user[13],'pickup_log':user[14],'drop_log':user[15],'paying_mode':user[16],'payment_status':user[17]}), 200
#                 else:
#                     return jsonify({'status':'Failed','message': 'No Data Found'}), 400
#             else:
#                 return jsonify({'status':'Failed','message': 'Invalid session'}), 401
            
#     except Exception as e:
#         return jsonify({'status':'Failed','message': 'We Are Facing Some Technical Issue Please try Again '+str(e)}), 500

@application.route('/gettravelhistory', methods=['POST'])
def gettravelhistory():
    try:
        # Get data from request
        data = request.get_json()
        uuid = data.get('uuid')

        if not uuid:
            return jsonify({'status': 'Failed', 'message': 'Session Id Invalid'}), 400

        # Check session validity
        if checkusersession(uuid):
            # Check Redis cache for travel history
            cached_travel_history = redis_cache.get(f"travel_history:{uuid}")

            if cached_travel_history:
                # Return cached data if available
                return jsonify({'status': 'Success', 'message': 'Fetch Success', 'travel_history': eval(cached_travel_history)}), 200

            # Fallback to database query if not in cache
            conn = create_connection()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT u.* FROM travel_history u JOIN ods o ON u.customer_id = o.user_id WHERE o.uuid = %s AND o.status = 'verified' AND u.riding_status != '4'",
                (uuid,)
            )
            user = cursor.fetchone()

            cursor.close()
            conn.close()

            if user:
                # Structure the result
                travel_history = {
                    'id': user[0],
                    'user_id': user[1],
                    'Customer_name': user[2],
                    'driverid': user[3],
                    'pickup_location': user[4],
                    'drop_location': user[5],
                    'pickup_time': str(user[6]),
                    'drop_time': str(user[7]),
                    'amount': user[8],
                    'date': str(user[9]),
                    'riding_status': user[10],
                    'otp': user[11],
                    'pickup_lati': user[12],
                    'drop_lati': user[13],
                    'pickup_log': user[14],
                    'drop_log': user[15],
                    'paying_mode': user[16],
                    'payment_status': user[17]
                }

                # Cache the result in Redis for 5 minutes
                redis_cache.setex(f"travel_history:{uuid}", 300, str(travel_history))

                # Return the result
                return jsonify({'status': 'Success', 'message': 'Fetch Success', 'travel_history': travel_history}), 200
            else:
                return jsonify({'status': 'Failed', 'message': 'No Data Found'}), 400
        else:
            return jsonify({'status': 'Failed', 'message': 'Invalid session'}), 401

    except Exception as e:
        return jsonify({'status': 'Failed', 'message': f'We Are Facing Some Technical Issue Please try Again {e}'}), 500


# Route for initilize travel
@application.route('/inittravel', methods=['POST'])
def inittravels():
    try:
        # Get data from request
        data = request.get_json()
        uuid = data.get('uuid')
        pickup_location = data.get('pickup_location','')
        drop_location = data.get('drop_location','')
        pickup_lati = data.get('pickup_lati','')
        pickup_longi = data.get('pickup_longi','')
        drop_lati = data.get('drop_lati','')
        drop_longi = data.get('drop_longi','')
        amount = data.get('amount','')
        paying_mode = data.get('paying_mode','')

        if checkusersession(uuid):
            if not pickup_location:
             return jsonify({'status':'Failed','message': 'Details are required!'}), 400

            
            opt = str(random.randint(10000, 99999))
            # Create a cursor and insert user into the database
            conn = create_connection()
            cursor = conn.cursor()
            customer_id = getuseridbyuuid(uuid)
            cursor.execute("SELECT riding_status FROM travel_history WHERE riding_status !='4' AND customer_id=%s",(customer_id,))
            check = cursor.fetchone()
            if not check:
                    cursor.execute("SELECT wallet_balance FROM users WHERE  user_id=%s",(customer_id,))
                    blc = cursor.fetchone()
                    if float(blc[0]) >= float(amount):
                        cursor.execute("INSERT INTO travel_history (customer_id, customer_name, driver_id, pickup_location, drop_location, pickup_time, drop_time, amount, date, riding_status, otp, pickup_latitude, drop_latitude, pickup_longitude, drop_longitude, paying_mode, payment_status ) SELECT u.user_id, u.full_name, %s, %s, %s, %s, %s, %s, NOW(), %s, %s, %s, %s, %s, %s, %s, %s FROM users u WHERE u.user_id = %s;", 
                        ('',pickup_location,drop_location,'','',amount,'0',opt,pickup_lati,drop_lati,pickup_longi,drop_longi,paying_mode,'0',customer_id))
                        
                        conn.commit()
                        cursor.close()
                        conn.close()
                    else:
                        return jsonify({'status':'Failed','message': 'You Wallet Balance Is Not Enough Please Add Balance'}), 400
        
            else:
                return jsonify({'status':'Failed','message': 'You Have Already a ride'}), 400    

            return jsonify({'status':'Success','message': 'Drive inisilized'}), 201
        else:
            return jsonify({'status':'Failed','message': 'Invalid session'}), 401
    except Exception as e:
        return jsonify({'status':'Failed','message': 'We Are Facing Some Technical Issue Please try Again'+str(e)}), 500

# Route drive accept ride
@application.route('/driver_accept', methods=['POST'])
def driveraccept():
    try:
        # Get data from request
        data = request.get_json()
        uuid = data.get('uuid')
        driver_id = getuseridbyuuid(uuid)
        id = data.get('id','')

        if checkusersession(uuid):
            if not uuid and not id:
                return jsonify({'status':'Failed','message': 'Details are required!'}), 400
            else:
                userid = 'CSH' + str(random.randint(10000, 99999))
                opt = str(random.randint(10000, 99999))
                # Create a cursor and insert user into the database
                conn = create_connection()
                cursor = conn.cursor()
                useri = getuseridbyuuid(uuid)
                cursor.execute("UPDATE travel_history SET driver_id = %s , riding_status=%s WHERE id=%s ;", (driver_id,'1',id))
                conn.commit()
                cursor.execute("SELECT riding_status FROM travel_history WHERE id=%s ;", (id,))
                check = cursor.fetchone()
            
                cursor.close()
                conn.close()
                if check[0] == '1':
                    return jsonify({'status':'Success','message': 'Drive Accepted'}), 201
                else:
                    return jsonify({'status':'Failed','message': 'Invalid session'}), 401
    except Exception as e:
        return jsonify({'status':'Failed','message': 'We Are Facing Some Technical Issue Please try Again'+str(e)}), 500

# Route for otp verify
@application.route('/otp_verify', methods=['POST'])
def otpverify():
    try:
        # Get data from request
        data = request.get_json()
        uuid = data.get('uuid')
        driver_id = getuseridbyuuid(uuid)
        id = data.get('id','')
        otp = data.get('otp','')

        if checkusersession(uuid):
            if not uuid and not otp and not id:
             return jsonify({'status':'Failed','message': 'Details are required!'}), 400

            # Create a cursor and insert user into the database
            conn = create_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT otp FROM travel_history WHERE id=%s",(id,))
            votp = cursor.fetchone()
            if  votp:
                if votp[0] == otp:
                    cursor.execute("UPDATE travel_history SET riding_status=%s WHERE id=%s ;", ('2',id))
                    conn.commit()
                    cursor.close()
                    conn.close()
                    return jsonify({'status':'Success','message': 'Otp Verified !'}), 201
                else:
                    return jsonify({'status':'Failed','message': 'Invalid Otp'}), 400
            else:
                return jsonify({'status':'Failed','message': 'Otp are required!'}), 400

            
        else:
            return jsonify({'status':'Failed','message': 'Invalid session'}), 401
    except Exception as e:
        return jsonify({'status':'Failed','message': 'We Are Facing Some Technical Issue Please try Again'+str(e)}), 500

# Route ride reach at destination
@application.route('/when_reached', methods=['POST'])
def whenreach():
    try:
        # Get data from request
        data = request.get_json()
        uuid = data.get('uuid')
        id = data.get('id','')

        if checkusersession(uuid):
            if not uuid and not id:
             return jsonify({'status':'Failed','message': 'Details are required!'}), 400

            userid = 'CSH' + str(random.randint(10000, 99999))
            opt = str(random.randint(10000, 99999))
            # Create a cursor and insert user into the database
            conn = create_connection()
            cursor = conn.cursor()
            useri = getuseridbyuuid(uuid)
            cursor.execute("UPDATE travel_history SET riding_status=%s WHERE id=%s ;", ('3',id))
            conn.commit()

            cursor.close()
            conn.close()

            return jsonify({'status':'Success','message': 'Reached The Destination'}), 201
        else:
            return jsonify({'status':'Failed','message': 'Invalid session'}), 401
    except Exception as e:
        return jsonify({'status':'Failed','message': 'We Are Facing Some Technical Issue Please try Again'+str(e)}), 500

# Route After Payment
@application.route('/afterpayment', methods=['POST'])
def afterpayment():
    try:
        # Get data from request
        data = request.get_json()
        uuid = data.get('uuid')
        id = data.get('id','')
        amount = data.get('amount','')

        if checkusersession(uuid):
            if not uuid and not id or not amount:
             return jsonify({'status':'Failed','message': 'Details are required!'}), 400
            
            conn = create_connection()
            cursor = conn.cursor()
            useri = getuseridbyuuid(uuid)
            cursor.execute("SELECT wallet_balance FROM users WHERE user_id=%s", (useri,))
            blc = cursor.fetchone()
            if blc:
                if Decimal(blc[0]) >=Decimal(amount):
                    newblc = Decimal(blc[0]) - Decimal(amount)
                    cursor.execute("SELECT driver_id FROM travel_history WHERE id=%s", (id,))
                    driver = cursor.fetchone()
                    if driver:
                        driver_id = driver[0]
                        cursor.execute("SELECT wallet_balance FROM users WHERE user_id=%s", (driver_id,))
                        dblc = cursor.fetchone()
                        if dblc:
                            newdblc = Decimal(dblc[0])+Decimal(amount)
                            cursor.execute("UPDATE travel_history SET payment_status=%s,riding_status=%s WHERE id=%s ;", ('1','4',id))
                            conn.commit()
                            cursor.execute("UPDATE users SET wallet_balance=%s WHERE user_id=%s ;", (newblc,useri))
                            conn.commit()
                            cursor.execute("UPDATE users SET wallet_balance=%s WHERE user_id=%s ;", (newdblc,driver_id))
                            conn.commit()
                            cursor.execute("INSERT INTO `transaction_history`(`user_id`, `amount`, `remark`, `reason`, `status`, `date`) VALUES (%s,%s,%s,%s,%s,now())"
                                       , (driver_id,amount,'ride completed amoutn received','1','Credit'))
                            conn.commit()
                            cursor.execute("INSERT INTO `transaction_history`(`user_id`, `amount`, `remark`, `reason`, `status`, `date`) VALUES (%s,%s,%s,%s,%s,now())"
                        , (useri,amount,'ride booking charge','1','Credit'))
                            conn.commit()


                            cursor.close()
                            conn.close()
                        
                            return jsonify({'status':'Success','message': 'Payment Successfull'}), 201
                        else:
                            return jsonify({'status':'Failed','message': 'Something Went Wrong1'}), 400
                    else:
                        return jsonify({'status':'Failed','message': 'Something Went Wrong2'}), 400
                else:
                    return jsonify({'status':'Failed','message': 'Low Wallet balance'}), 400    
            else:
                 return jsonify({'status':'Failed','message': 'Unable to get balance'}), 400      
        else:
            return jsonify({'status':'Failed','message': 'Invalid session'}), 401
    except Exception as e:
        return jsonify({'status':'Failed','message': 'We Are Facing Some Technical Issue Please try Again'+str(e)}), 500

# Route for add driver
@application.route('/adddriver', methods=['POST'])
def adddrivers():
    try:
        # Get data from request
        data = request.get_json()
        uuid = data.get('uuid')
        driver_id = getuseridbyuuid(uuid)
        vehicle_model = data.get('vehicle_model')
        vehicle_no = data.get('vehicle_no')
        image = data.get('image')


        if checkusersession(uuid):
            
            if not vehicle_model or not vehicle_no or not image:
                return jsonify({'status':'Failed','message': 'Details are required!'}), 400

            # Create a cursor and insert user into the database
            conn = create_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT u.driver_status, d.driver_id FROM users AS u LEFT JOIN driver AS d ON u.user_id = d.driver_id WHERE u.user_id =%s;",(driver_id,))
            isdriver = cursor.fetchone()
            if isdriver[0] == '1':
                if isdriver[1] is None:
                    cursor.execute("INSERT INTO `driver`(`driver_id`, `full_name`, `address`, `city`, `state`, `zip_code`, `mobile_no`, `email`, `vehicle_model`, `vehicle_no`, `image`, `date`) SELECT u.user_id,u.full_name,u.address,u.city,u.state,u.zip_code,u.mobile_no,u.email_id,%s,%s,%s,now() FROM users as u WHERE u.user_id =%s", (vehicle_model,vehicle_no,image,driver_id))
                    conn.commit()

                    cursor.close()
                    conn.close()
                else:
                     return jsonify({'status':'Failed','message': 'You Are Allready Registered As Driver'}), 400   
            else:
                 return jsonify({'status':'Failed','message': 'You Are Not Registered As Driver'}), 400   

            return jsonify({'status':'Success','message': 'Driver added successfully!'}), 201
        else:
            return jsonify({'status':'Failed','message': 'Invalid session'}), 401
    
    except Exception as e:
        return jsonify({'status':'Failed','message': 'We Are Facing Some Technical Issue Please try Again '+str(e)}), 500

#Route for get driver info
@application.route('/getdriverinfo', methods=['POST'])
def getdriverinfos():
    try:
        # Get data from request
        data = request.get_json()
        uuid = data.get("uuid")
        driverid = data.get("driverid",getuseridbyuuid(uuid))

        if not driverid:
         return jsonify({'status':'Failed','message': 'No session id provided'}), 400

        # Create a cursor and check the user in the database
        if checkusersession(uuid):
            conn = create_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM `driver` WHERE driver_id = %s", (driverid,))
            user = cursor.fetchone()

            cursor.close()
            conn.close()

            if user:
            # Return the user ID if login is successful
                return jsonify({'status':'Success','message': 'Fetch Success', 'driver_id': user[1],'full_name':user[2],'address':user[3],'city':user[4],'state':user[5],'zip_code':user[6],
                            'mobile':user[7],'email':user[8],'vehicle_model':user[9],'vehicle_no':user[10],'image':user[11],'date':user[12]}), 200
            else:
                return jsonify({'status':'Failed','message': 'No Data Found'}), 400
        else:
            return jsonify({'status':'Failed','message': 'Invalid session'}), 401
    except Exception as e:
        return jsonify({'message': 'We Are Facing Some Technical Issue Please try Again '+str(e)}), 500

# Route check driver accepted or not
@application.route('/cehckdriverallo', methods=['POST'])
def cehckdriverallow():
    try:
        # Get data from request
        data = request.get_json()
        uuid = data.get('uuid')
        id = data.get("id")

        if not uuid:
            return jsonify({'status':'Failed','message': 'Session Missmatched'}), 400

        # Create a cursor and check the user in the database
        if checkusersession(uuid):
            conn = create_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT th.driver_id, th.riding_status,d.full_name,d.vehicle_no,d.image FROM travel_history AS th LEFT JOIN driver AS d ON th.driver_id = d.driver_id WHERE th.id =%s;", (id,))
            user = cursor.fetchone()
            if user:
                if user[1] == '1':
                    cursor.close()
                    conn.close()
                    return jsonify({'status':'Success','message': 'Driver Conform', 'driver_id': user[0],'full_name':user[2],'vehicle_no':user[3],"image":user[4]}), 200
                else:
                    return jsonify({'status':'Failed','message': 'Waiting For Driver'}), 400
            else:
                return jsonify({'status':'Failed','message': 'Invalid Id'}), 400
        else:
            return jsonify({'status':'Failed','message': 'Invalid Session'}), 401

    except Exception as e:
        return jsonify({'message': 'We Are Facing Some Technical Issue Please try Again '+str(e)}), 500

# Route for get travel history
@application.route('/riding_request', methods=['POST'])
def ridingrequest():
    try:
            # Get data from request
            data = request.get_json()
            uuid = data.get('uuid')

            if not uuid:
                return jsonify({'status':'Failed','message': 'Session Id Invalid'}), 400

            # Create a cursor and check the user in the database
            if checkusersession(uuid):   
                conn = create_connection()
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM `travel_history` ")
                rows = cursor.fetchall()

                cursor.close()
                conn.close()

                if rows:
                    column_names = [desc[0] for desc in cursor.description]
                    users = [dict(zip(column_names, row)) for row in rows]
                    return jsonify(users), 200 
                else:
                    return jsonify({'status':'Failed','message': 'No Data Found'}), 400
            else:
                return jsonify({'status':'Failed','message': 'Invalid session'}), 401
            
    except Exception as e:
        return jsonify({'status':'Failed','message': 'We Are Facing Some Technical Issue Please try Again '+str(e)}), 500

# Route for Ride History
@application.route('/ridehistoryuser', methods=['POST'])
def ridehistoryss():
    try:
            # Get data from request
            data = request.get_json()
            uuid = data.get('uuid')

            if not uuid:
                return jsonify({'status':'Failed','message': 'Session Id Invalid'}), 400

            # Create a cursor and check the user in the database
            if checkusersession(uuid):   
                conn = create_connection()
                userid = getuseridbyuuid(uuid)
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM `travel_history` WHERE customer_id =%s",(userid,))
                rows = cursor.fetchall()

                cursor.close()
                conn.close()

                if rows:
                    column_names = [desc[0] for desc in cursor.description]
                    users = [dict(zip(column_names, row)) for row in rows]
                    return jsonify(users), 200 
                else:
                    return jsonify({'status':'Failed','message': 'No Data Found'}), 400
            else:
                return jsonify({'status':'Failed','message': 'Invalid session'}), 401
            
    except Exception as e:
        return jsonify({'status':'Failed','message': 'We Are Facing Some Technical Issue Please try Again '+str(e)}), 500

# Route for Ride History
@application.route('/gettransactionuser', methods=['POST'])
def gettransactionusers():
    try:
            # Get data from request
            data = request.get_json()
            uuid = data.get('uuid')

            if not uuid:
                return jsonify({'status':'Failed','message': 'Session Id Invalid'}), 400

            # Create a cursor and check the user in the database
            if checkusersession(uuid):   
                conn = create_connection()
                userid = getuseridbyuuid(uuid)
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM `transaction_history` WHERE user_id = %s",(userid,))
                rows = cursor.fetchall()

                cursor.close()
                conn.close()

                if rows:
                    column_names = [desc[0] for desc in cursor.description]
                    users = [dict(zip(column_names, row)) for row in rows]
                    return jsonify(users), 200 
                else:
                    return jsonify({'status':'Failed','message': 'No Data Found'}), 400
            else:
                return jsonify({'status':'Failed','message': 'Invalid session'}), 401
            
    except Exception as e:
        return jsonify({'status':'Failed','message': 'We Are Facing Some Technical Issue Please try Again '+str(e)}), 500

@application.route('/get_driver_riding_history', methods=['POST'])
def getdriverridinghistory():
    try:
            # Get data from request
            data = request.get_json()
            uuid = data.get('uuid')

            if not uuid:
                return jsonify({'status':'Failed','message': 'Session Id Invalid'}), 400

            # Create a cursor and check the user in the database
            if checkusersession(uuid):   
                conn = create_connection()
                cursor = conn.cursor()
                uid = getuseridbyuuid(uuid)
                cursor.execute("select * FROM travel_history where driver_id = %s",(uid,))
                rows = cursor.fetchall()

                cursor.close()
                conn.close()

                if rows:
                    column_names = [desc[0] for desc in cursor.description]
                    users = [dict(zip(column_names, row)) for row in rows]
                    return jsonify(users), 200 
                else:
                    return jsonify({'status':'Failed','message': 'No Data Found'}), 401
            else:
                return jsonify({'status':'Failed','message': 'Invalid session'}), 401
            
    except Exception as e:
        return jsonify({'status':'Failed','message': 'We Are Facing Some Technical Issue Please try Again '+str(e)}), 500

@application.route('/update_profile', methods=['POST'])
def updateprofile():
    try:
        # Get data from request
        data = request.get_json()
        uuid = data.get('uuid')
        name = data.get('name','')
        mobile = data.get('mobile','')
        address = data.get('address','')
        city = data.get('city','')
        state = data.get('state','')
        zip = data.get('zip','')
        gender = data.get('gender','')
        userid = getuseridbyuuid(uuid)
        if checkusersession(uuid):
            if not uuid or not name or not address or not mobile or not gender or not state or not zip or not state or not city:
             return jsonify({'status':'Failed','message': 'Details are required!'}), 400


            # Create a cursor and insert user into the database
            conn = create_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT user_id FROM users WHERE user_id=%s",(userid,))
            pas = cursor.fetchone()
    
            if pas[0] == userid:
                cursor.execute("UPDATE users SET full_name=%s,mobile_no=%s,gender=%s,address=%s,city=%s,state=%s,zip_code=%s WHERE user_id=%s",(name,mobile,gender,address,city,state,zip,userid))
                conn.commit()

                cursor.close()
                conn.close()
                return jsonify({'status':'Success','message': 'Profile Updated Success'}), 201
            else:
                return jsonify({'status':'Failed','message': 'Invalid Session'}), 401
            
            
        else:
            return jsonify({'status':'Failed','message': 'Invalid session'}), 401
        
    except Exception as e:
        return jsonify({'status':'Failed','message': 'We Are Facing Some Technical Issue Please try Again '+str(e)}), 500







# --------------------------------- ADMIN PANEL ---------------------

# Route for Admin login
@application.route('/admin_login', methods=['POST'])
def adminlogin():
    try:
        # Get data from request
        data = request.get_json()
        emailorphone = data.get('emailid')
        password = data.get('password')
        randomtextfokey = generate_unique_string()

        if not emailorphone or not password:
            return jsonify({'status': 'Failed', 'message': 'User Credential are required!'}), 400

        # Generate a One-Time-Secret (OTS)
        ots = str(random.randint(10000, 99999))

        # Database connection
        conn = create_connection()
        cursor = conn.cursor()
        # Authenticate user and check blocking status before
        cursor.execute(
            "SELECT admin_id FROM `admin` WHERE admin_id= %s AND admin_password= %s",
            (emailorphone, password)
        )
        user = cursor.fetchone()

        if not user:
            return jsonify({'status': 'Failed', 'message': 'Invalid Credential!'}), 400

        admin_id = user

        # Fix: Ensure user_id is passed as a tuple
        cursor.execute("SELECT ots FROM `ods` WHERE user_id = %s AND status = 'verified'", (emailorphone,))
        check = cursor.fetchone()

        if not check:
            cursor.execute(
                "INSERT INTO `ods`(`mobile_no`, `ots`, `user_id`, `uuid`, `status`, `dates`) VALUES (%s, %s, %s, %s, %s, %s)",
                (emailorphone, ots, emailorphone, randomtextfokey, 'verified', date.today())
            )
        else:
            cursor.execute("UPDATE `ods` SET `status`='unverified' WHERE user_id = %s", (emailorphone,))
            cursor.execute(
                "INSERT INTO `ods`(`mobile_no`, `ots`, `user_id`, `uuid`, `status`, `dates`) VALUES (%s, %s, %s, %s, %s, %s)",
                (emailorphone, ots, emailorphone, randomtextfokey, 'verified', date.today())
            )

        # Commit changes
        conn.commit()

        # Close resources
        cursor.close()
        conn.close()

        # Return success response with UUID
        return jsonify({'status': 'Success', 'message': 'Login successful!', 'uuid': randomtextfokey}), 200

    except mysql.connector.Error as e:
        # Handle MySQL-specific errors
        return jsonify({'status': 'Failed', 'message': "We Are Facing Some Technical Issue Please try Again"}), 500

    except Exception as e:
        # Handle general exceptions
        return jsonify({'status': 'Failed', 'message': 'We Are Facing Some Technical Issue Please try Again '+str(e)}), 500

# Route for Ride History
@application.route('/ridehistory', methods=['POST'])
def ridehistorys():
    try:
            # Get data from request
            data = request.get_json()
            uuid = data.get('uuid')

            if not uuid:
                return jsonify({'status':'Failed','message': 'Session Id Invalid'}), 400

            # Create a cursor and check the user in the database
            if checkusersession(uuid):   
                conn = create_connection()
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM `travel_history`")
                rows = cursor.fetchall()

                cursor.close()
                conn.close()

                if rows:
                    column_names = [desc[0] for desc in cursor.description]
                    users = [dict(zip(column_names, row)) for row in rows]
                    return jsonify(users), 200 
                else:
                    return jsonify({'status':'Failed','message': 'No Data Found'}), 400
            else:
                return jsonify({'status':'Failed','message': 'Invalid session'}), 401
            
    except Exception as e:
        return jsonify({'status':'Failed','message': 'We Are Facing Some Technical Issue Please try Again '+str(e)}), 500

# Route for Transaction History
@application.route('/transaction', methods=['POST'])
def transactions():
    try:
            # Get data from request
            data = request.get_json()
            uuid = data.get('uuid')

            if not uuid:
                return jsonify({'status':'Failed','message': 'Session Id Invalid'}), 400

            # Create a cursor and check the user in the database
            if checkusersession(uuid):   
                conn = create_connection()
                cursor = conn.cursor()
                cursor.execute("SELECT d.id,u.email_id,d.amount,d.remark,d.reason,d.status,d.date FROM transaction_history d JOIN users u ON d.user_id = u.user_id")
                rows = cursor.fetchall()

                cursor.close()
                conn.close()

                if rows:
                    column_names = [desc[0] for desc in cursor.description]
                    users = [dict(zip(column_names, row)) for row in rows]
                    return jsonify(users), 200 
                else:
                    return jsonify({'status':'Failed','message': 'No Data Found'}), 400
            else:
                return jsonify({'status':'Failed','message': 'Invalid session'}), 401
            
    except Exception as e:
        return jsonify({'status':'Failed','message': 'We Are Facing Some Technical Issue Please try Again '+str(e)}), 500

#Route for Incom
@application.route('/incominfo', methods=['POST'])
def infominfos():
    try:
        # Get data from request
        data = request.get_json()
        uuid = data.get("uuid")

        if not uuid:
         return jsonify({'status':'Failed','message': 'No session provided'}), 400

        # Create a cursor and check the user in the database
        if checkusersession(uuid):
            conn = create_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT (SELECT COUNT(CASE WHEN driver_status = 0 THEN 0 END) FROM users ) AS total_customers, (SELECT COUNT(CASE WHEN driver_status = 1 THEN 1 END) FROM users ) AS total_drivers, (SELECT COALESCE(SUM(amount),0) FROM transaction_history where reason='0') AS total_amount, (SELECT COALESCE(SUM(CASE WHEN DATE(date) = CURDATE() THEN amount ELSE 0 END),0) FROM transaction_history where reason='0') AS total_amount_today,  (SELECT COUNT(*) FROM travel_history) AS total_rides, (SELECT COUNT(CASE WHEN DATE(date) = CURDATE() THEN 1 END) FROM travel_history) AS today_rides")
            rows = cursor.fetchone()

            cursor.close()
            conn.close()

            if rows:
                
                return jsonify({"total_customers":rows[0],"total_drivers":rows[1],"total_amount":rows[2],"total_amount_today":rows[3],"total_rides":rows[4],"today_rides":rows[5]}), 200 
            else:
                return jsonify({'status':'Failed','message': 'No Data Found'}), 400
        else:
            return jsonify({'status':'Failed','message': 'Invalid session'}), 401
    except Exception as e:
        return jsonify({'message': 'We Are Facing Some Technical Issue Please try Again'+str(e)}), 500

# Route for Users History
@application.route('/usersinfo', methods=['POST'])
def usersinfos():
    try:
            # Get data from request
            data = request.get_json()
            uuid = data.get('uuid')

            if not uuid:
                return jsonify({'status':'Failed','message': 'Session Id Invalid'}), 400

            # Create a cursor and check the user in the database
            if checkusersession(uuid):   
                conn = create_connection()
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM `users`")
                rows = cursor.fetchall()

                cursor.close()
                conn.close()

                if rows:
                    column_names = [desc[0] for desc in cursor.description]
                    users = [dict(zip(column_names, row)) for row in rows]
                    return jsonify(users), 200 
                else:
                    return jsonify({'status':'Failed','message': 'No Data Found'}), 400
            else:
                return jsonify({'status':'Failed','message': 'Invalid session'}), 401
            
    except Exception as e:
        return jsonify({'status':'Failed','message': 'We Are Facing Some Technical Issue Please try Again '+str(e)}), 500

#Route for add balance to user from admin panel 
@application.route('/addbalance', methods=['POST'])
def addbalance():
    try:
        # Get data from request
        data = request.get_json()
        uuid = data.get('uuid')
        email = data.get('email','')
        balance = data.get('balance','')
        userid = getuseridbyuuid(uuid)
        if checkusersession(uuid):
            if not uuid or not email or not balance:
             return jsonify({'status':'Failed','message': 'Details are required!'}), 400


            # Create a cursor and insert user into the database
            conn = create_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT wallet_balance,user_id FROM users WHERE email_id=%s",(email,))
            pas = cursor.fetchone()
    
            if pas:
                currentblc = Decimal(balance) + Decimal(pas[0])
                user_ids = pas[1]
                cursor.execute("UPDATE users SET wallet_balance=%s WHERE user_id=%s",(currentblc,user_ids))
                conn.commit()
                cursor.execute("INSERT INTO `transaction_history`(`user_id`, `amount`, `remark`, `reason`, `status`, `date`) VALUES (%s,%s,%s,%s,%s,now())"
                , (user_ids,balance,'payment received by admin','0','Credit'))
                conn.commit()
                cursor.close()
                conn.close()
                return jsonify({'status':'Success','message': 'Balance Updated Success'}), 201
            else:
                return jsonify({'status':'Failed','message': 'Invalid Session'}), 401
            
            
        else:
            return jsonify({'status':'Failed','message': 'Invalid session'}), 401
        
    except Exception as e:
        return jsonify({'status':'Failed','message': 'We Are Facing Some Technical Issue Please try Again '+str(e)}), 500

# Route For debit blance from users in admin panel
@application.route('/debitbalance', methods=['POST'])
def debitbalances():
    try:
        # Get data from request
        data = request.get_json()
        uuid = data.get('uuid')
        email = data.get('email','')
        balance = data.get('balance','')
        userid = getuseridbyuuid(uuid)
        if checkusersession(uuid):
            if not uuid or not email or not balance:
             return jsonify({'status':'Failed','message': 'Details are required!'}), 400


            # Create a cursor and insert user into the database
            conn = create_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT wallet_balance,user_id FROM users WHERE email_id=%s",(email,))
            pas = cursor.fetchone()
    
            if pas:
                if Decimal(pas[0]) >= Decimal(balance):
                    currentblc = Decimal(pas[0]) - Decimal(balance) 
                    user_ids = pas[1]
                    cursor.execute("UPDATE users SET wallet_balance=%s WHERE user_id=%s",(currentblc,user_ids))
                    conn.commit()
                    cursor.execute("INSERT INTO `transaction_history`(`user_id`, `amount`, `remark`, `reason`, `status`, `date`) VALUES (%s,%s,%s,%s,%s,now())"
                , (user_ids,balance,'payment debited by admin','0','Debit'))
                    conn.commit()
                    cursor.close()
                    conn.close()
                    return jsonify({'status':'Success','message': 'Balance Debited Success'}), 201
                else:
                     return jsonify({'status':'Failed','message': 'Not Enough Balance To Debit'}), 401   
                
            else:
                return jsonify({'status':'Failed','message': 'Invalid Session'}), 401
            
            
        else:
            return jsonify({'status':'Failed','message': 'Invalid session'}), 401
        
    except Exception as e:
        return jsonify({'status':'Failed','message': 'We Are Facing Some Technical Issue Please try Again '+str(e)}), 500












data = pd.read_csv('uber.csv')

# Data Cleaning
# Remove null values and outliers
data = data.dropna()
data = data[(data['fare_amount'] > 0) & (data['fare_amount'] < 500)]
data = data[(data['pickup_longitude'] >= -180) & (data['pickup_longitude'] <= 180)]
data = data[(data['pickup_latitude'] >= -90) & (data['pickup_latitude'] <= 90)]
data = data[(data['dropoff_longitude'] >= -180) & (data['dropoff_longitude'] <= 180)]
data = data[(data['dropoff_latitude'] >= -90) & (data['dropoff_latitude'] <= 90)]

# Feature Engineering
# Extract temporal features from pickup_datetime
data['pickup_datetime'] = pd.to_datetime(data['pickup_datetime'])
data['hour'] = data['pickup_datetime'].dt.hour
data['day_of_week'] = data['pickup_datetime'].dt.dayofweek
data['month'] = data['pickup_datetime'].dt.month

# Compute distance using the Haversine formula
def haversine_distance(lat1, lon1, lat2, lon2):
    R = 6371  # Radius of Earth in kilometers
    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlambda = np.radians(lon2 - lon1)
    a = np.sin(dphi/2)*2 + np.cos(phi1) * np.cos(phi2) * np.sin(dlambda/2)*2
    return R * 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

data['distance'] = haversine_distance(
    data['pickup_latitude'], data['pickup_longitude'],
    data['dropoff_latitude'], data['dropoff_longitude']
)

# Select features and target
features = ['distance', 'hour', 'day_of_week', 'month', 'passenger_count']
target = 'fare_amount'

# Train-test split
X_train, X_test, y_train, y_test = train_test_split(data[features], data[target], test_size=0.2, random_state=42)

# Train a LightGBM model
model = LGBMRegressor(n_estimators=100, learning_rate=0.1, random_state=42)
model.fit(X_train, y_train)

# Evaluate the model
y_pred = model.predict(X_test)
rmse = np.sqrt(mean_squared_error(y_test, y_pred))
#print(f'RMSE: {rmse:.2f}')

def predict_fare(pickup_lat, pickup_lon, dropoff_lat, dropoff_lon, datetime, passenger_count, is_event):
    distance = haversine_distance(pickup_lat, pickup_lon, dropoff_lat, dropoff_lon)
    hour = datetime.hour
    day_of_week = datetime.weekday()
    month = datetime.month

    # Base feature engineering
    input_features = pd.DataFrame({
        'distance': [distance],
        'hour': [hour],
        'day_of_week': [day_of_week],
        'month': [month],
        'passenger_count': [passenger_count]
    })


    # Predict base fare
    base_fare = model.predict(input_features)[0]

    # Create a dummy demand_patterns DataFrame
    hours = list(range(24))  # Hours in a day (0 to 23)
    days = list(range(7))    # Days of the week (0 = Monday, 6 = Sunday)

    # Generate random trip counts for each hour and day
    np.random.seed(42)  # For reproducibility
    dummy_data = {
      'hour': [hour for hour in hours for _ in days],
      'day_of_week': days * len(hours),
      'trip_count': np.random.randint(50, 500, len(hours) * len(days))  # Random trip counts
    }

    demand_patterns = pd.DataFrame(dummy_data)

    # Apply dynamic pricing adjustments
    # 1. Demand-Supply Multiplier
    # Apply demand-supply multiplier
    try:
        trip_count = demand_patterns.loc[
            (demand_patterns['hour'] == hour) &
            (demand_patterns['day_of_week'] == day_of_week),
            'trip_count'
        ].values[0]
        demand_multiplier = 1 + (trip_count / demand_patterns['trip_count'].max())
    except IndexError:
        #Handle case where no data exists for this specific time
        demand_multiplier = 1.0

    # 2. Event Multiplier
    event_multiplier = 1.2 if is_event else 1.0

    # 3. Traffic Multiplier
   # traffic_multiplier = 1 + (traffic_delay / 10)  # Example: +10% per 10 min delay

    # Final fare calculation
    adjusted_fare = base_fare * demand_multiplier * event_multiplier
    #* traffic_multiplier
    return round(adjusted_fare, 2)

@application.route('/getvehicleinfo', methods=['POST'])
def getvehicleinfos():
    conn = create_connection()
    cursor = conn.cursor()

    try:
        # Query to fetch data
        data = request.get_json()
        uuid = data.get('uuid')

        if checkusersession(uuid):
            query = "SELECT pasanger_count, vehicle_name, base_price, vehicle_image FROM vehicle_id"
            cursor.execute(query)

            # Fetch all rows and column names
            rows = cursor.fetchall()
            column_names = [desc[0] for desc in cursor.description]

            # Convert rows to dictionaries
            result = [dict(zip(column_names, row)) for row in rows]

            pickup_lat = float(data.get('pickup_lat'))
            pickup_lon = float(data.get('pickup_lon'))
            dropoff_lat = float(data.get('dropoff_lat'))
            dropoff_lon = float(data.get('dropoff_lon'))
            now = datetime.now()
            datetime_input = datetime(now.year, now.month, now.day, now.hour, now.minute)  # Example: 3:30 PM on Nov 23, 2024
            start_date = now.strftime('%Y-%m-%d')
            end_date = now.strftime('%Y-%m-%d')
            holidays = cal.holidays(start=start_date, end=end_date)
            is_event = datetime_input.date() in holidays.date

            # Prepare filtered response with calculated fare
            filtered_result = []
            for row in result:
                filtered_row = {
                    "vehicle_image": row["vehicle_image"],
                    "vehicle_name": row["vehicle_name"],
                    "passenger_count": row["pasanger_count"],
                    "fare": predict_fare(pickup_lat, pickup_lon, dropoff_lat, dropoff_lon, datetime_input, int(row["pasanger_count"]), is_event),
                }
                filtered_result.append(filtered_row)  # Ensure this is inside the loop

            return jsonify(filtered_result)
        else:
            return jsonify({'status': 'Failed', 'message': 'Invalid session'}), 401
    except Exception as e:
        return jsonify({"error": str(e)})
    finally:
        # Close connection
        cursor.close()
        conn.close()


if __name__ == "__main__":
    application.run(port=5000, debug=True)
