package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func listS3BackupFiles(s3Bucket, s3KeyPrefix, region string) ([]string, error) {
	// Load AWS configuration
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("unable to load AWS config: %w", err)
	}

	// Create S3 client
	s3Client := s3.NewFromConfig(cfg)

	// List objects in the S3 bucket
	output, err := s3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(s3Bucket),
		Prefix: aws.String(s3KeyPrefix),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list objects in S3 bucket: %w", err)
	}

	var files []string
	for _, object := range output.Contents {
		files = append(files, *object.Key)
	}

	return files, nil
}

func downloadFromS3(s3Bucket, s3Key, destinationPath, region string) error {
	// Load AWS configuration
	cfg, err := config.LoadDefaultConfig(context.TODO(), config.WithRegion(region))
	if err != nil {
		return fmt.Errorf("unable to load AWS config: %w", err)
	}

	// Create S3 downloader
	s3Downloader := manager.NewDownloader(s3.NewFromConfig(cfg))

	// Create a file to write to
	file, err := os.Create(destinationPath)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", destinationPath, err)
	}
	defer file.Close()

	// Download the file from S3
	_, err = s3Downloader.Download(context.TODO(), file, &s3.GetObjectInput{
		Bucket: aws.String(s3Bucket),
		Key:    aws.String(s3Key),
	})
	if err != nil {
		return fmt.Errorf("failed to download file from S3: %w", err)
	}

	fmt.Printf("Downloaded backup from s3://%s/%s to %s\n", s3Bucket, s3Key, destinationPath)
	return nil
}

func restoreDatabase(dbName, dbUser, dbPassword, dbHost string, dbPort int, backupFilePath string) error {
	// Set environment variable for PostgreSQL password
	os.Setenv("PGPASSWORD", dbPassword)

	// Run the pg_restore command to restore the database
	cmd := exec.Command("pg_restore", "-h", dbHost, "-p", fmt.Sprintf("%d", dbPort), "-U", dbUser, "-d", dbName, "-c", "-F", "c", backupFilePath)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to restore database %s: %w", dbName, err)
	}

	fmt.Printf("Database %s restored successfully from %s\n", dbName, backupFilePath)
	return nil
}

func restoreAllDatabasesFromS3(dbHost string, dbPort int, dbUser, dbPassword, s3Bucket, s3KeyPrefix, region string) error {
	// List all backup files in the S3 bucket
	backupFiles, err := listS3BackupFiles(s3Bucket, s3KeyPrefix, region)
	if err != nil {
		return err
	}

	// Iterate over the backup files and restore each database
	for _, s3Key := range backupFiles {
		fmt.Printf("Processing backup file: %s\n", s3Key)

		// Download the backup file from S3
		backupFilename := filepath.Base(s3Key)
		backupFilePath := filepath.Join(os.TempDir(), backupFilename)
		if err := downloadFromS3(s3Bucket, s3Key, backupFilePath, region); err != nil {
			log.Printf("Failed to download backup file %s: %v", s3Key, err)
			continue
		}
		defer os.Remove(backupFilePath) // Clean up the file after restoration

		// Extract the database name from the backup filename (assuming it's formatted like dbname_backup_timestamp.sql)
		dbName := backupFilename[:len(backupFilename)-27] // Remove the "_backup_timestamp.sql" suffix
		if err := restoreDatabase(dbName, dbUser, dbPassword, dbHost, dbPort, backupFilePath); err != nil {
			log.Printf("Failed to restore database %s: %v", dbName, err)
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
	region := "ap-south-1"
	s3KeyPrefix := os.Getenv("S3_DIR")

	// Restore all databases from S3 backups
	if err := restoreAllDatabasesFromS3(dbHost, dbPort, dbUser, dbPassword, s3Bucket, s3KeyPrefix, region); err != nil {
		log.Fatalf("Error: %v", err)
	}
}
