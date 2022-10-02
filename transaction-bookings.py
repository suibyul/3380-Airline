import sys, psycopg2, threading, math
from typing import NamedTuple

with open('password.txt') as f:
    lines = [line.rstrip() for line in f]
    
username = lines[0]
pg_password = lines[1]

conn = psycopg2.connect(database = "COSC3380", user = username, password = pg_password)
cursor = conn.cursor()

class passenger_info(NamedTuple):
    passenger_id: str
    flight_id: str
    book_ref: str
    ticket_no: int


successful_transaction = 0
unsuccessful_transaction = 0
failed_transaction = 0
bookings_record = 0
flights_record = 0
ticket_record = 0
ticket_flights_record = 0
total_amount = 0
null_var = 'NULL'
name = ''
sqlfile = open("transaction-bookings.sql", "w")


lock = threading.Lock()

def addtodb(info):
    global successful_transaction, unsuccessful_transaction, lock, total_amount, bookings_record, flights_record, ticket_record, ticket_flights_record, null_var, name, sqlfile, failed_transaction
    # start transaction 
    lock.acquire()

    try:
        #print(info[0])
        for i in info:
            # check if passenger_id and flight_id invalid. passenger_id != NULL, flight_id exist in flights table
            # if invalid: continue for loop
            
            if i.passenger_id == None:
                failed_transaction += 1
                continue

            #check_flight_id = "IF EXISTS (SELECT * FROM flights WHERE flights.flight_id = " + i.flight_id + ") THEN TRUE ELSE FALSE END;"

            check_flight_id = "SELECT COUNT(*) FROM flights WHERE flights.flight_id = " + i.flight_id + ";"
            cursor.execute(check_flight_id)
            sqlfile.write(check_flight_id + '\n')
            flight_id_exists = cursor.fetchone()[0]
            if flight_id_exists == 0: 
                failed_transaction +=1
                continue

            # if passes, update bookings table: book_ref = i.book_ref, book_date = any date before (current_timestamp), total_amount = 0
            #insert_bookings = "INSERT INTO bookings(book_ref, book_date, total_amount) VALUES (" + i.book_ref + ", current_timestamp, " + total_amount + ");"
            insert_bookings = "INSERT INTO bookings(book_ref, book_date, total_amount) VALUES (%s, current_timestamp, %s)"%(i.book_ref, total_amount)
            cursor.execute(insert_bookings)
            sqlfile.write(insert_bookings + '\n')
            bookings_record += 1

            # check for unsuccesful transaction: check seats_available from flights table
            check_seats = "SELECT (CASE WHEN flights.seats_available > 0 THEN 1 ELSE 0 END) FROM flights WHERE flights.flight_id = " + i.flight_id
            cursor.execute(check_seats)
            sqlfile.write(check_seats + '\n')
            seats_avail = cursor.fetchone()[0]

            # if available, update seats_available and seats_booked
            if seats_avail == 1:
                update_seats = "UPDATE flights SET seats_available = seats_available - 1, seats_booked = seats_booked + 1 WHERE flight_id = " + i.flight_id
                cursor.execute(update_seats)
                sqlfile.write(update_seats + '\n')
                successful_transaction += 1
                flights_record += 1

            # if seats_available == 0, unsuccesful transaction += 1, and continue for loop
            if seats_avail == 0:
                unsuccessful_transaction += 1
                continue
            
            # then insert into ticket table: ticket_no, book_ref, passenger_id, passenger_name, email, phone

            insert_ticket = "INSERT INTO ticket(ticket_no, book_ref, passenger_id, passenger_name, email, phone) VALUES (%s, %s, %s, '%s', %s, %s)"%(i.ticket_no, i.book_ref, i.passenger_id, name, null_var, null_var)
            #insert_ticket = "INSERT INTO ticket(ticket_no, book_ref, passenger_id) VALUES (%s, %s, %s)"%(i.ticket_no, i.book_ref, i.passenger_id)
            cursor.execute(insert_ticket)
            sqlfile.write(insert_ticket + '\n')
            ticket_record += 1

            # also update ticket_flights: ticket_no, flight_id, fare_conditions = economy, amount = 0
            insert_ticket_flights = "INSERT INTO ticket_flights(ticket_no, flight_id, fare_conditions, amount) VALUES (%s, %s, '%s', %s)"%(i.ticket_no, i.flight_id, "Economy", total_amount)
            cursor.execute(insert_ticket_flights)
            sqlfile.write(insert_ticket_flights + '\n')
            ticket_flights_record += 1

            conn.commit()

    finally:
        lock.release()


def addtodbN(info):
    global successful_transaction, unsuccessful_transaction, lock, total_amount, bookings_record, flights_record, ticket_record, ticket_flights_record, null_var, name, sqlfile, failed_transaction
    # start transaction 
    lock.acquire()

    try:
        #print(info[0])
        for i in info:
            # check if passenger_id and flight_id invalid. passenger_id != NULL, flight_id exist in flights table
            # if invalid: continue for loop
            
            if i.passenger_id == None:
                failed_transaction += 1
                continue

            #check_flight_id = "IF EXISTS (SELECT * FROM flights WHERE flights.flight_id = " + i.flight_id + ") THEN TRUE ELSE FALSE END;"

            check_flight_id = "SELECT COUNT(*) FROM flights WHERE flights.flight_id = " + i.flight_id + ";"
            cursor.execute(check_flight_id)
            sqlfile.write(check_flight_id + '\n')
            flight_id_exists = cursor.fetchone()[0]
            if flight_id_exists == 0: 
                failed_transaction +=1
                continue

            # if passes, update bookings table: book_ref = i.book_ref, book_date = any date before (current_timestamp), total_amount = 0
            #insert_bookings = "INSERT INTO bookings(book_ref, book_date, total_amount) VALUES (" + i.book_ref + ", current_timestamp, " + total_amount + ");"
            insert_bookings = "INSERT INTO bookings(book_ref, book_date, total_amount) VALUES (%s, current_timestamp, %s)"%(i.book_ref, total_amount)
            cursor.execute(insert_bookings)
            sqlfile.write(insert_bookings + '\n')
            bookings_record += 1

            # check for unsuccesful transaction: check seats_available from flights table
            check_seats = "SELECT (CASE WHEN flights.seats_available > 0 THEN 1 ELSE 0 END) FROM flights WHERE flights.flight_id = " + i.flight_id
            cursor.execute(check_seats)
            sqlfile.write(check_seats + '\n')
            seats_avail = cursor.fetchone()[0]

            # if available, update seats_available and seats_booked
            if seats_avail == 1:
                update_seats = "UPDATE flights SET seats_available = seats_available - 1, seats_booked = seats_booked + 1 WHERE flight_id = " + i.flight_id
                cursor.execute(update_seats)
                sqlfile.write(update_seats + '\n')
                successful_transaction += 1
                flights_record += 1

            # if seats_available == 0, unsuccesful transaction += 1, and continue for loop
            if seats_avail == 0:
                unsuccessful_transaction += 1
                continue
            
            # then insert into ticket table: ticket_no, book_ref, passenger_id, passenger_name, email, phone

            insert_ticket = "INSERT INTO ticket(ticket_no, book_ref, passenger_id, passenger_name, email, phone) VALUES (%s, %s, %s, '%s', %s, %s)"%(i.ticket_no, i.book_ref, i.passenger_id, name, null_var, null_var)
            #insert_ticket = "INSERT INTO ticket(ticket_no, book_ref, passenger_id) VALUES (%s, %s, %s)"%(i.ticket_no, i.book_ref, i.passenger_id)
            cursor.execute(insert_ticket)
            sqlfile.write(insert_ticket + '\n')
            ticket_record += 1

            # also update ticket_flights: ticket_no, flight_id, fare_conditions = economy, amount = 0
            insert_ticket_flights = "INSERT INTO ticket_flights(ticket_no, flight_id, fare_conditions, amount) VALUES (%s, %s, '%s', %s)"%(i.ticket_no, i.flight_id, "Economy", total_amount)
            cursor.execute(insert_ticket_flights)
            sqlfile.write(insert_ticket_flights + '\n')
            ticket_flights_record += 1

    finally:
        lock.release()

def main():

    text = sys.argv[1]
    splitinput = text.split(';')

    inputline = splitinput[0]
    filename = inputline[6:len(inputline)]

    transactionline = splitinput[1]
    transaction = transactionline[12:len(transactionline)]

    threadline = splitinput[2]
    numthreads = threadline[8:len(threadline)]
    threadnum = int(numthreads)

    file = open(filename, "r")

    passenger_id_list = []
    flight_id_list = []
    book_ref_list = []
    ticket_no_list = []

    count = 0

    infolist_tuple = [] # size = 110
    infolist_dict = []

    for i in file.readlines():
        i = i.rstrip('\n')
        if i[0] != "p":
            line = i.split(',')
            count += 1
            book_ref_str = str(count)
            ticket_no_list.append(count)
            book_ref_list.append(count)
            passenger_id_list.append(line[0])
            flight_id_list.append(line[1])
            pi = passenger_info(passenger_id = line[0], flight_id = line[1], book_ref = book_ref_str, ticket_no = count)
            info_list_dict = {
                "pi" : line[0],
                "fi" : line[1],
                "br" : count,
                "tn" : count
            }
            infolist_dict.append(info_list_dict)
            infolist_tuple.append(pi)


    x = 0
    a_list = []
    for i in range(threadnum):
        sublist = []
        a_list.append(sublist)

    while(x != count):
        for i in a_list:
            if x == count:
                break
            i.append(infolist_tuple[x])
            x+=1

    thread_list = []

    if transaction == 'y':
        for i in a_list:
            thread = threading.Thread(target = addtodb, args=(i,))
            thread_list.append(thread)
            thread.start()

        for i in thread_list:
            thread.join()

    

    if transaction == 'n':
        #print("no transactions")
        for i in a_list:
            thread = threading.Thread(target = addtodbN, args=(i,))
            thread_list.append(thread)
            thread.start()

        for i in thread_list:
            thread.join()


    print("Successful Trans:", successful_transaction)
    print("Unsuccessful Trans:", unsuccessful_transaction)
    print("# records update for table ticket:", ticket_record)
    print("# records update for table ticket_flights:", ticket_flights_record)
    print("# records update for table bookings:", bookings_record)
    print("# records update for table flights:", flights_record)
    
if __name__ == "__main__":
    main()