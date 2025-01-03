from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
from pymongo import MongoClient
from elasticsearch import Elasticsearch

# MongoDB Configuration
MONGO_URI = "mongodb://localhost:27017"
DB_NAME = "resume_scoring_db"  # Database name for storing resume data
COLLECTION_NAME = "scored_resumes"  # Collection for storing scored resumes

# Elasticsearch Configuration
ES_HOST = "es01"
ES_PORT = "9200"
ES_INDEX = "resumes"

# Elasticsearch API Key (for authentication)
ES_API_KEY = "QWMtb0xaUUJoTk4zTWEwdHhYaWY6eV9QcTRlNVJRNVdkTVlBdEhUdmN5QQ=="

# MongoDB Client Initialization
client = MongoClient(MONGO_URI)  # Connect to MongoDB using the URI
db = client[DB_NAME]  # Access the database
collection = db[COLLECTION_NAME]  # Access the collection for scored resumes

# Elasticsearch Client Initialization
es = Elasticsearch(
    f"https://{ES_HOST}:{ES_PORT}",
    api_key=ES_API_KEY,
    verify_certs=True,
)


def store_scored_resume_in_mongo(resume):
    """
    Store the scored resume in MongoDB.

    Parameters:
    resume (dict): A dictionary representing the scored resume.

    Returns:
    None
    """
    collection.insert_one(resume)  # Insert the resume into MongoDB collection
    print(f"Scored resume stored in MongoDB: {resume}")  # Log the storage action


def score_resume(resume):
    """
    Score the resume based on skills with weighted categories.

    Parameters:
    resume (dict): A dictionary representing the resume to be scored.

    Returns:
    dict: The resume with the calculated score added.
    """
    # Define skill categories
    technical_skills = [
        "Python",
        "Java",
        "C++",
        "JavaScript",
        "SQL",
        "NoSQL",
        "Machine Learning",
        "Data Science",
        "Deep Learning",
        "TensorFlow",
        "Keras",
        "PyTorch",
        "Docker",
        "Kubernetes",
    ]
    soft_skills = [
        "Leadership",
        "Communication",
        "Teamwork",
        "Problem-Solving",
        "Critical Thinking",
        "Time Management",
        "Negotiation",
        "Presentation Skills",
        "Customer Service",
    ]
    programming_languages = [
        "Python",
        "Java",
        "C++",
        "JavaScript",
        "R",
        "Scala",
        "Go",
        "Ruby",
        "Swift",
        "C#",
        "PHP",
    ]
    language_skills = [
        "English",
        "French",
        "Spanish",
        "Mandarin",
        "German",
        "Italian",
        "Portuguese",
        "Japanese",
        "Russian",
        "Arabic",
    ]

    # Define weights for each category
    weights = {
        "technical": 3,  # Technical skills are given a weight of 3
        "soft": 1,  # Soft skills are given a weight of 1
        "languages": 2,  # Language skills are given a weight of 2
    }

    total_score = 0  # Initialize the total score

    # Extract skills from the resume and score based on categories
    skills = resume.get("skills", "").split(",")
    for skill in skills:
        skill = skill.strip()
        if skill in technical_skills:
            total_score += weights["technical"]
        elif skill in soft_skills:
            total_score += weights["soft"]
        elif skill in programming_languages:
            total_score += weights["languages"]
        elif skill in language_skills:
            total_score += weights["languages"]

    resume["score"] = total_score  # Add the total score to the resume
    return resume


def create_spark_session():
    """
    Create and configure a Spark session for reading from Kafka, MongoDB, and Elasticsearch.

    Returns:
    SparkSession: Configured Spark session.
    """
    return (
        SparkSession.builder.appName("ResumeScoringStreaming")
        .config("spark.mongodb.input.uri", MONGO_URI)
        .config("spark.mongodb.output.uri", MONGO_URI)
        .config("spark.es.nodes", ES_HOST)
        .config("spark.es.port", ES_PORT)
        .config("spark.es.index.auto.create", "true")
        .config("spark.es.nodes.wan.only", "true")
        .getOrCreate()
    )


def define_kafka_source(spark):
    """
    Define Kafka source to read resume data from the Kafka topic.

    Parameters:
    spark (SparkSession): The Spark session object.

    Returns:
    DataFrame: A streaming DataFrame representing the Kafka source.
    """
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "resume_topic")
        .load()
    )


def define_schema():
    """
    Define the schema for Kafka messages.

    Returns:
    StructType: The schema for the Kafka messages.
    """
    return StructType(
        [
            StructField("filename", StringType(), True),
            StructField("file_path", StringType(), True),
            StructField("skills", StringType(), True),
        ]
    )


def process_stream(df, batch_id):
    """
    Process each batch of resumes from the Kafka stream, scoring and storing them in MongoDB and Elasticsearch.

    Parameters:
    df (DataFrame): The DataFrame representing the current batch of data.
    batch_id (str): The batch identifier.

    Returns:
    None
    """
    resumes = df.collect()  # Collect the DataFrame rows into a list

    bulk_data = []  # Initialize a list to hold bulk data for Elasticsearch

    for row in resumes:
        resume = row.asDict()  # Convert each row to a dictionary
        scored_resume = score_resume(resume)  # Score the resume
        store_scored_resume_in_mongo(
            scored_resume
        )  # Store the scored resume in MongoDB

        # Prepare bulk data for Elasticsearch ingestion
        action = {"index": {"_index": ES_INDEX}}

        # Add the action and document to the bulk data list
        bulk_data.append(action)
        bulk_data.append(scored_resume)

    # Perform a bulk insert to Elasticsearch
    es.bulk(operations=bulk_data, pipeline="ent-search-generic-ingestion")


if __name__ == "__main__":
    # Initialize the Spark session
    spark = create_spark_session()

    # Define Kafka source and schema for streaming data
    kafka_df = define_kafka_source(spark)
    schema = define_schema()

    # Parse Kafka messages and extract the "resume" field
    parsed_df = (
        kafka_df.selectExpr("CAST(value AS STRING)")
        .select(from_json(col("value"), schema).alias("resume"))
        .select("resume.*")
    )

    # Process the stream and apply scoring logic
    query = parsed_df.writeStream.foreachBatch(process_stream).start()

    query.awaitTermination()  # Wait for the stream to finish
