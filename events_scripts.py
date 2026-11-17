#POPULATING DATA SCRIPTS
from supabase_helpers.supabase_connection import supabase

async def update_event_table():
    people= []
    events = supabase.rpc("get_all_unique_events",{"p_event_type":"Cells"}).execute().data
    for event in events:
        person = supabase.rpc("get_cell_leader",{"p_email":event.get("event_leader_email"),"p_fullname":event.get("event_leader")}).execute().data
        if len(person) > 0:
            leader = person[0]
            leader_fullname = f"{leader.get("Name")} {leader.get("Surname")}" 
            if leader.get("LeaderPath[0]") == "692d97e550555e9dfdccea4e":
                leader1 = "Gavin Enslin"
            elif leader.get("LeaderPath[0]") == "692d97ec50555e9dfdccecee":
                leader1 = "Vicky Enslin"

            leader12Query = supabase.table("People").select("*").eq("_id", leader.get("LeaderPath[1]")).limit(1).execute().data
            leader12 = ""
            leader12email= ''
            if len(leader12Query) > 0:
                leader12 = f"{leader12Query[0].get("Name")} {leader12Query[0].get("Surname")}"
                leader12email = leader12Query[0].get("Email")
            leaders = {
                "leader_at_1": leader1,
                "leader_at_12": leader12,
                "leader_at_12_email": leader12email,
            }
            people.append({
                "leader": leader_fullname,
                "leader1": leader1,
                "leader12": leader12,
                "leader12email": leader12email,
                "event": event
            })   
            updated = supabase.table("events").update(leaders).eq("event_id", event.get("event_id")).execute().data
        else:
            people.append(event.get("event_leader") + " " + event.get("event_name"))
    return people        
    
    

async def update_event_table():
    people= []
    events = [
        {"event_name": "Ben Mpasi - Springfield - Open cell - Wednesday","leader_at_1":"Gavin Enslin","leader_at_12":"Kenny Bebel","leader_at_12_email":"kennybebel@gmail.com"},
        {"event_name": "Blessed Manana - La Rochelle - Open cell - Wednesday","leader_at_1":"Gavin Enslin","leader_at_12":"Kenny Bebel","leader_at_12_email":"kennybebel@gmail.com"},
        {"event_name": "Cynthia Bebel - Die Fakkel High School - School cell - Thursday","leader_at_1":"Vicky Enslin","leader_at_12":"","leader_at_12_email":""},
        {"event_name": "Cynthia Bebel - Die Fakkel High School - School cell - Tuesday","leader_at_1":"Vicky Enslin","leader_at_12":"","leader_at_12_email":""},
        {"event_name": "Cynthia Bebel - Rosettenville - Open cell - Tuesday","leader_at_1":"Vicky Enslin","leader_at_12":"","leader_at_12_email":""},
        {"event_name": "Cynthia Bebel - Rosettenville Primary - School cell - Wednesday","leader_at_1":"Vicky Enslin","leader_at_12":"","leader_at_12_email":""},
        {"event_name": "Danielle Holtzhausen - Monument High School - School cell - Friday","leader_at_1":"Vicky Enslin","leader_at_12":"Kayla Enslin","leader_at_12_email":"enslinkayla@gmail.com"},
        {"event_name": "Denise van de Sandt - Robertsham - Open cell - Tuesday","leader_at_1":"Vicky Enslin","leader_at_12":"","leader_at_12_email":""},
        {"event_name": "Elvine Kapila - Forest High School - School cell - Thursday","leader_at_1":"Vicky Enslin","leader_at_12":"Sasha-Lee Enslin","leader_at_12_email":"sashlee4@gmail.com"},
        {"event_name": "Elvine Kapila - Rosettenville - Open cell - Wednesday","leader_at_1":"Vicky Enslin","leader_at_12":"Sasha-Lee Enslin","leader_at_12_email":"sashlee4@gmail.com"},
        {"event_name": "Elvine Kapila - Forest High School - School cell - Thursday","leader_at_1":"Vicky Enslin","leader_at_12":"Sasha-Lee Enslin","leader_at_12_email":"sashlee4@gmail.com"},
    {"event_name": "Elvine Kapila - Rosettenville - Open cell - Wednesday","leader_at_1":"Vicky Enslin","leader_at_12":"Sasha-Lee Enslin","leader_at_12_email":"sashlee4@gmail.com"},
    {"event_name": "Louange Ngoyi - Rosettenville - Open cell - Wednesday","leader_at_1":"Vicky Enslin","leader_at_12":"","leader_at_12_email":""},
    {"event_name": "Nhlakanipho Madlanga - Die Fakkel High School - Tuesday","leader_at_1":"Gavin Enslin","leader_at_12":"Nash Bobo Mbankumuna","leader_at_12_email":"mbankumunabobo@gmail.com"},
        {"event_name": "Nhlakanipho Madlanga - Turffontein - Open cell - Wednesday","leader_at_1":"Gavin Enslin","leader_at_12":"Nash Bobo Mbankumuna","leader_at_12_email":"mbankumunabobo@gmail.com"},
    {"event_name": "Nicholas Shamshum - Tedderfield - Open cell - Saturday","leader_at_1":"Gavin Enslin","leader_at_12":"Shane van der Walt","leader_at_12_email":"shane@theactivechurch.org"},  
 {"event_name": "Tracy Bebel - Die Fakkel High School - Open cell - Thursday","leader_at_1":"Vicky Enslin","leader_at_12":"Kayla Enslin","leader_at_12_email":"enslinkayla@gmail.com"},
    {"event_name": "Tracy Bebel - Die Fakkel High School - Open cell - Tuesday","leader_at_1":"Vicky Enslin","leader_at_12":"Kayla Enslin","leader_at_12_email":"enslinkayla@gmail.com"},
    {"event_name": "Vicky Enslin - Glenanda - Closed cell - Monday","leader_at_1":"","leader_at_12":"","leader_at_12_email":""},
    ]
    for event in events[2:]:
        leaders = {
            "leader_at_1": event.get("leader_at_1"),
            "leader_at_12": event.get("leader_at_12"),
            "leader_at_12_email": event.get("leader_at_12_email")
        }
        # 1. Fetch only the first occurrence matching the event name
        record = (
            supabase.table("events")
            .select("event_id")  # Replace "event_id" with your actual primary key column name
            .eq("event_name", event.get("event_name"))
            .limit(1)
            .execute()
        )

        # 2. Check if a row was found, then update it using its specific ID
        if record.data:
            row_id = record.data[0]["event_id"]

            response = (
                supabase.table("events")
                .update(leaders)
                .eq("event_id", row_id)  # Updates only this specific row
                .execute()
            )
            updated_data = response.data
        else:
            print("No matching event found.")
    return updated_data      
    

async def update_session_attendees():
    #adding event name to every attendee
    i = 0;
    update = {}
    attendees = supabase.table("event_session_attendees").select("*").is_("event_name", "null").execute().data
    for attendee in attendees:
        event = supabase.table("events").select("event_name").eq("event_id", attendee.get("event_id")).limit(1).execute().data
        if event:
            update = supabase.table("event_session_attendees").update({"event_name": event[0]["event_name"]}).eq("id", attendee.get("id")).execute()
            if update.data:
                i+=1
    return i
    #adding event name to every session
    # count = 0
    # updated = {}
    # sessions = supabase.table("event_sessions").select("*").is_("event_name", "null").execute().data
    # for session in sessions:
    #     event = supabase.table("events").select("event_name").eq("event_id", session.get("event_id")).limit(1).execute().data
    #     if event:
            
    #         updated = supabase.table("event_sessions").update({"event_name": event[0]["event_name"]}).eq("session_id", session.get("session_id")).execute()
    #         if updated.data:
    #             count+=1
    # print(updated)
    return count


def add_person_id():
    """
    adds person id to the event_session_attendees table
    """
    attendees = supabase.table("event_attendees").select("*").is_("person_id", "null").execute().data
    changed = 0
    total = 0
    people=[]
    for att in attendees:
        print(att.get("full_name"))
        total+=1
        params = {
            "p_email": att.get("email"),
            "p_fullname":att.get("full_name")
        } 
        person = supabase.rpc("match_people",params).execute().data
        if person:
            supabase.table("event_attendees").update({"person_id": person[0]["person_id"]}).eq("attendee_id", att.get("attendee_id")).execute()
            changed += 1
        else:
            people.append(att.get("full_name"))
    print(f"Done!: {changed}/{total}")
    print("People not found:", people)
add_person_id()