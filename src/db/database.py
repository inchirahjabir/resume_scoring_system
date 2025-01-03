from pymongo import MongoClient

# MongoDB URI and database/collection configuration
MONGO_URI = "mongodb://localhost:27017"  # URI for connecting to MongoDB
DB_NAME = "resume_scoring_db"  # Name of the MongoDB database
COLLECTION_NAME = "resumes"  # Name of the collection where resumes will be stored

# MongoDB client setup
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]


def store_resume(resume):
    """
    Stores a resume document in the MongoDB collection.

    Parameters:
    resume (dict): A dictionary containing the resume data to be stored,
                   including fields like 'name', 'skills', and 'experience'.

    Returns:
    None
    """
    collection.insert_one(
        resume
    )  # Insert the resume document into the MongoDB collection
    print("Resume stored in MongoDB")
