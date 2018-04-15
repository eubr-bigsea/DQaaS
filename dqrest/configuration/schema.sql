DROP TABLE IF EXISTS assessment_file;
DROP TABLE IF EXISTS requests;
DROP TABLE IF EXISTS requests_assessment;
DROP TABLE IF EXISTS requests_profiling;
CREATE TABLE assessment_file (user_id text, file_id text PRIMARY KEY, created_on text);
CREATE TABLE requests (user_id text, type text, uuid text PRIMARY KEY, done bit, submit_timestamp text, completed_timestamp text, error bit);
CREATE TABLE requests_assessment (uuid text PRIMARY KEY, file_id text);
CREATE TABLE requests_profiling (uuid text PRIMARY KEY, args text);
