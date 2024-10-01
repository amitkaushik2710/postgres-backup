package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	_ "github.com/lib/pq"
)

func getDatabaseList(dbHost string, dbPort int, dbUser, dbPassword string) ([]string, error) {
	// Connect to the PostgreSQL server
	connStr := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=postgres sslmode=disable", dbHost, dbPort, dbUser, dbPassword)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}
	defer db.Close()

	// Query the list of databases
	rows, err := db.Query("SELECT datname FROM pg_database WHERE datistemplate = false;")
	if err != nil {
		return nil, fmt.Errorf("failed to query databases: %w", err)
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var dbName string
		if err := rows.Scan(&dbName); err != nil {
			return nil, fmt.Errorf("failed to scan database name: %w", err)
		}
		databases = append(databases, dbName)
	}

	return databases, nil
}

func backupDatabase(dbName, dbUser, dbPassword, dbHost string, dbPort int) (string, error) {
	// Set environment variable for PostgreSQL password
	os.Setenv("PGPASSWORD", dbPassword)

	// Create a backup file name with a timestamp
	backupFilename := fmt.Sprintf("%s_backup_%s.sql", dbName, time.Now().Format("20060102_150405"))
	backupFilePath := filepath.Join(os.TempDir(), backupFilename)

	// Run the pg_dump command to backup the database
	cmd := exec.Command("pg_dump", "-h", dbHost, "-p", fmt.Sprintf("%d", dbPort), "-U", dbUser, "-F", "c", "-f", backupFilePath, dbName)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("failed to backup database: %w", err)
	}

	return backupFilePath, nil
}

func uploadToS3(backupFilePath, s3Bucket, s3KeyPrefix, region string) error {
	// Load AWS configuration
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(region),
	)
	if err != nil {
		return fmt.Errorf("unable to load AWS config: %w", err)
	}

	// Create S3 client
	s3Client := s3.NewFromConfig(cfg)

	// Open the backup file
	file, err := os.Open(backupFilePath)
	if err != nil {
		return fmt.Errorf("failed to open backup file: %w", err)
	}
	defer file.Close()

	// Create S3 key
	backupFilename := filepath.Base(backupFilePath)
	s3Key := fmt.Sprintf("%s/%s", s3KeyPrefix, backupFilename)

	// Upload the backup file to S3
	_, err = s3Client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(s3Bucket),
		Key:    aws.String(s3Key),
		Body:   file,
		ACL:    types.ObjectCannedACLPrivate,
	})
	if err != nil {
		return fmt.Errorf("failed to upload to S3: %w", err)
	}

	fmt.Printf("Backup successful: %s uploaded to s3://%s/%s\n", backupFilename, s3Bucket, s3Key)
	return nil
}

func backupAllDatabasesToS3(dbHost string, dbPort int, dbUser, dbPassword, s3Bucket, s3KeyPrefix, region string) error {
	// Get the list of databases
	databases, err := getDatabaseList(dbHost, dbPort, dbUser, dbPassword)
	if err != nil {
		return err
	}

	// Loop over each database and backup
	for _, dbName := range databases {
		fmt.Printf("Backing up database: %s\n", dbName)

		// Backup the database
		backupFilePath, err := backupDatabase(dbName, dbUser, dbPassword, dbHost, dbPort)
		if err != nil {
			log.Printf("Failed to backup database %s: %v", dbName, err)
			continue
		}
		defer os.Remove(backupFilePath) // Clean up the file after uploading

		// Upload the backup to S3
		if err := uploadToS3(backupFilePath, s3Bucket, s3KeyPrefix, region); err != nil {
			log.Printf("Failed to upload backup for database %s: %v", dbName, err)
			continue
		}
	}

	return nil
}

func main() {
	// Database and S3 configuration
	dbHost := "localhost"
	dbPort := 5432
	dbUser := "postgres"
	dbPassword := "postgres"
	s3Bucket := "kmf-db"
	s3KeyPrefix := fmt.Sprintf("%d", time.Now().Unix())
	region := "ap-south-1"

	// Perform backups for all databases
	if err := backupAllDatabasesToS3(dbHost, dbPort, dbUser, dbPassword, s3Bucket, s3KeyPrefix, region); err != nil {
		log.Fatalf("Error: %v", err)
	}
}
