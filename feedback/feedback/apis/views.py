from django.shortcuts import render, HttpResponse
import json
# Create your views here.
def storeFormData(request, name,feeling , preferred):
    # Connect to db
    import MySQLdb
    conn = MySQLdb.connect(host="10.129.23.41",
                           user="writer",
                           passwd="datapool",
                           db="datapool")
    x = conn.cursor()

    try:
        # insert statement
        x.execute("""INSERT INTO feedback (name,feeling,preferred) VALUES (%s,%s,%s)""", (name, feeling,preferred))
        conn.commit()
    except:
        conn.rollback()
        

    conn.close()

    return HttpResponse(json.dumps({'response':'success'}))

