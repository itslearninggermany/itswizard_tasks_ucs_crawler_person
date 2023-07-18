package main

import (
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatchlogs"
	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/itslearninggermany/awsBrooker"
	"github.com/itslearninggermany/imses"
	"github.com/itslearninggermany/itswizard_basic"
	"github.com/jinzhu/gorm"
	"github.com/pkg/errors"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	cwl           *cloudwatchlogs.CloudWatchLogs
	logGroupName  = "UCSPersonCrawler"
	logStreamName = ""
	sequenceToken = ""
)

func init() {
	sess, err := session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Region: aws.String("eu-central-1"), // london
		},
	})

	if err != nil {
		panic(err)
	}

	cwl = cloudwatchlogs.New(sess)

	err = ensureLogGroupExists(logGroupName)
	if err != nil {
		panic(err)
	}

}

func sendLog2(logtext string) {
	fmt.Println(logtext)
}

func sendLog(logtext string) {
	var logQueue []*cloudwatchlogs.InputLogEvent
	logQueue = append(logQueue, &cloudwatchlogs.InputLogEvent{
		Message:   &logtext,
		Timestamp: aws.Int64(time.Now().UnixNano() / int64(time.Millisecond)),
	})

	input := cloudwatchlogs.PutLogEventsInput{
		LogEvents:    logQueue,
		LogGroupName: &logGroupName,
	}

	if sequenceToken == "" {
		err := createLogStream()
		if err != nil {
			panic(err)
		}
	} else {
		input = *input.SetSequenceToken(sequenceToken)
	}

	input = *input.SetLogStreamName(logStreamName)

	resp, err := cwl.PutLogEvents(&input)
	if err != nil {
		log.Println(err)
	}

	if resp != nil {
		sequenceToken = *resp.NextSequenceToken
	}

	logQueue = []*cloudwatchlogs.InputLogEvent{}
	time.Sleep(250 * time.Millisecond)
}

// ensureLogGroupExists first checks if the log group exists,
// if it doesn't it will create one.
func ensureLogGroupExists(name string) error {
	resp, err := cwl.DescribeLogGroups(&cloudwatchlogs.DescribeLogGroupsInput{})
	if err != nil {
		return err
	}

	for _, logGroup := range resp.LogGroups {
		if *logGroup.LogGroupName == name {
			return nil
		}
	}

	_, err = cwl.CreateLogGroup(&cloudwatchlogs.CreateLogGroupInput{
		LogGroupName: &name,
	})
	if err != nil {
		return err
	}

	_, err = cwl.PutRetentionPolicy(&cloudwatchlogs.PutRetentionPolicyInput{
		RetentionInDays: aws.Int64(14),
		LogGroupName:    &name,
	})

	return err
}

// createLogStream will make a new logStream with a random uuid as its name.
func createLogStream() error {
	name := uuid.New().String()
	_, err := cwl.CreateLogStream(&cloudwatchlogs.CreateLogStreamInput{
		LogGroupName:  &logGroupName,
		LogStreamName: &name,
	})

	logStreamName = name

	return err
}

// processQueue will process the log queue
func processQueue(queue *[]string) error {
	var logQueue []*cloudwatchlogs.InputLogEvent

	for {
		if len(*queue) > 0 {
			for _, item := range *queue {
				logQueue = append(logQueue, &cloudwatchlogs.InputLogEvent{
					Message:   &item,
					Timestamp: aws.Int64(time.Now().UnixNano() / int64(time.Millisecond)),
				})
			}

			*queue = []string{}
		}

		if len(logQueue) > 0 {
			input := cloudwatchlogs.PutLogEventsInput{
				LogEvents:    logQueue,
				LogGroupName: &logGroupName,
			}

			if sequenceToken == "" {
				err := createLogStream()
				if err != nil {
					panic(err)
				}
			} else {
				input = *input.SetSequenceToken(sequenceToken)
			}

			input = *input.SetLogStreamName(logStreamName)

			resp, err := cwl.PutLogEvents(&input)
			if err != nil {
				log.Println(err)
			}

			if resp != nil {
				sequenceToken = *resp.NextSequenceToken
			}

			logQueue = []*cloudwatchlogs.InputLogEvent{}
		}

		time.Sleep(time.Second * 5)
	}

}

// Für die Nebenläufigkeit
var dataInProcess map[string]bool

func lockUser(personid string) {
	dataInProcess[personid] = true
}

func unlockUser(personid string) {
	dataInProcess[personid] = false
}

func isUserlocked(personid string) bool {
	return dataInProcess[personid]
}

type ucsSyncSetup struct {
	UCSSetupAdminSpecification              bool
	UCSSetupAdminLastNames                  []string
	UCSSetupMakeTeacherFirstnameToOneLetter bool
	UCSSetupMakeStudentFirstnameToOneLetter bool
	UCSSetupMakeStudentFirstnameToOneName   bool
	UCSSetupMakeTeacherFirstnameToOneName   bool
	UCSSetupPeronFullFirstNames             []string
	UCSSetupEmailNotToSync                  bool
	UCSSetupSyncDisabled                    bool
	itsl                                    *imses.Request
	db                                      *gorm.DB
	dbClient                                *gorm.DB
	OUSelect                                bool     // Nur bestimmte OUs übertragen
	Ous                                     []string // Alle OUS die übertragen werden müssen
}

var logCache []string
var lock sync.Mutex
var loggingtime time.Time

func main() {
	loggingtime = time.Now()

	sendLog("Start UCS Person Crawler")

	// Einrichten für die Nebenläufigkeit
	dataInProcess = make(map[string]bool)

	// Datenbank einrichten
	var databaseConfig []itswizard_basic.DatabaseConfig
	b, _ := awsBrooker.DownloadFileFromBucket("brooker", "admin/databaseconfig.json")
	err := json.Unmarshal(b, &databaseConfig)
	if err != nil {
		panic("Error by reading database file " + err.Error())
		return
	}
	allDatabases := make(map[string]*gorm.DB)
	for i := 0; i < len(databaseConfig); i++ {
		database, err := gorm.Open(databaseConfig[i].Dialect, databaseConfig[i].Username+":"+databaseConfig[i].Password+"@tcp("+databaseConfig[i].Host+")/"+databaseConfig[i].NameOrCID+"?charset=utf8&parseTime=True&loc=Local")
		if err != nil {
			log.Println(err)
		}
		allDatabases[databaseConfig[i].NameOrCID] = database
	}

	var ucsSyncSetupMap map[uint]ucsSyncSetup

	log.Println("Writing in Database that the service is running")
	var runningService RunningService
	err = allDatabases["Client"].Where("service_name = ?", "UCSPersonCrawler").Find(&runningService).Error
	if err != nil {
		panic(err)
		return
	}
	runningService.LastRun = time.Now().String()
	err = allDatabases["Client"].Save(&runningService).Error
	if err != nil {
		panic(err)
		return
	}

	log.Println("Check if new Clients are need to add to the loop")

	ucsSyncSetupMap = make(map[uint]ucsSyncSetup)

	fmt.Println("Get all Univention Services from Database")
	var univentionServices []itswizard_basic.UniventionService
	err = allDatabases["Client"].Where("run_person_crawler = ?", true).Find(&univentionServices).Error
	if err != nil {
		sendLog(err.Error() + "while reading run_with_update = true")
		log.Println(err)
		allDatabases["Client"].Save(&itswizard_basic.UcsDatabaseToItslearning{
			InstitutionID: 0,
			Error:         err.Error(),
		})
		return
	}

	fmt.Println("Create UCS Setup to range throw it")

	for _, univentionSerice := range univentionServices {

		log.Println(univentionSerice.InsitutionID)
		db := allDatabases[strconv.Itoa(int(univentionSerice.InsitutionID))]
		//Get All IMSES DAta
		var imsesSetup itswizard_basic.ImsesSetup
		err = db.Last(&imsesSetup).Error
		if err != nil {
			sendLog("Error while getting imses setup: " + err.Error() + "Stop programm")
			log.Println(err)
			return
		}
		itsl := imses.NewImsesService(imses.NewImsesServiceInput{
			Username: imsesSetup.Username,
			Password: imsesSetup.Password,
			Url:      imsesSetup.Endpoint,
		})

		//Get UCSSetup
		var ucssetup itswizard_basic.UniventionSetup
		err = db.Last(&ucssetup).Error
		if err != nil {
			sendLog("Error while getting univention setup: " + err.Error() + "Stop programm")
			log.Println(err)
			return
		}

		var adminLastnames []string
		if ucssetup.AdminSpecification {
			var adminspec []itswizard_basic.UniventionAdminSpecifiaction
			err = db.Find(&adminspec).Error
			if err != nil {
				sendLog("Error while getting UniventionAdminSpecifiaction: " + err.Error() + "Stop programm")
				log.Println(err)
				return
			}
			for _, data := range adminspec {
				adminLastnames = append(adminLastnames, data.AdminLastName)
			}
		}

		var fullFirstNames []itswizard_basic.UniventionPersonFullFirstName
		err = db.Find(&fullFirstNames).Error
		if err != nil {
			fmt.Println("There is no UniventionPersonFullFirstname: " + err.Error())
			log.Println(err)
			if err.Error() != "record not found" {
				sendLog("Error while getting UniventionPersonFullFirstName: " + err.Error() + "Stop programm")
				log.Println(err)
				return
			}
		}

		var firstnames []string
		for _, name := range fullFirstNames {
			firstnames = append(firstnames, name.PersonSyncKey)
		}

		var ous []string
		if univentionSerice.SelectOrganisations {
			var organisationSelects []itswizard_basic.UniventionOrganisationSelect
			err = allDatabases["Client"].Where("institution_id = ? and active = ?", univentionSerice.InsitutionID, true).Find(&organisationSelects).Error
			if err != nil {
				fmt.Println("There is no UniventionOrganisationSelect: " + err.Error())
				log.Println(err)
				if err.Error() != "record not found" {
					sendLog("Error while getting UniventionPersonFullFirstName: " + err.Error() + "Stop programm")
					log.Println(err)
					return
				}
			}
			for _, selectOrganisation := range organisationSelects {
				ous = append(ous, selectOrganisation.OUName)
			}
		}

		ucsSyncSetupMap[univentionSerice.InsitutionID] = ucsSyncSetup{
			UCSSetupAdminSpecification:              ucssetup.AdminSpecification,
			UCSSetupAdminLastNames:                  adminLastnames,
			UCSSetupPeronFullFirstNames:             firstnames,
			UCSSetupMakeTeacherFirstnameToOneLetter: ucssetup.MakeTeacherFirstnameToOneLetter,
			UCSSetupMakeStudentFirstnameToOneLetter: ucssetup.MakeStudentFirstnameToOneLetter,
			UCSSetupMakeStudentFirstnameToOneName:   ucssetup.MakeStudentFirstnameToOneName,
			UCSSetupMakeTeacherFirstnameToOneName:   ucssetup.MakeTeacherFirstnameToOneName,
			UCSSetupEmailNotToSync:                  ucssetup.EmailNotToSync,
			UCSSetupSyncDisabled:                    ucssetup.SyncDisable,
			itsl:                                    itsl,
			db:                                      db,
			dbClient:                                allDatabases["Client"],
			OUSelect:                                univentionSerice.SelectOrganisations,
			Ous:                                     ous,
		}
	}

	//Start sync to itslearning
	for institutionID, setup := range ucsSyncSetupMap {

		// Import
		var importDatas []itswizard_basic.UniventionPerson
		err = setup.db.Where("to_import = 1 and error = 0").Order("updated_at").Find(&importDatas).Error
		if err != nil {
			if err.Error() != "record not found" {
				sendLog("Error while getting UniventionPerson to import: " + err.Error() + "Stop programm")
				log.Println(err)
				return
			}
		}
		for _, importdatao := range importDatas {
			if importdatao.Data != "" {
				sendLog(ucsImportUser(setup, importdatao, institutionID))
			}
		}

		// Delete
		var deleteData []itswizard_basic.UniventionPerson
		err = setup.db.Where("to_delete = 1 and success = 0 and error = 0").Limit(500).Find(&deleteData).Error
		if err != nil {
			if err.Error() != "record not found" {
				sendLog("Error while getting UniventionPerson to delete: " + err.Error() + "Stop programm")
				log.Println(err)
				return
			}
		}
		for _, v := range deleteData {
			if v.Data != "" {
				sendLog(ucsDeleteUser(setup, v, institutionID))
			}
		}

		// Update
		var updateDatas []itswizard_basic.UniventionPerson
		err = setup.db.Where("to_update = 1 and error = 0 ").Limit(200).Find(&updateDatas).Error
		if err != nil {
			if err.Error() != "record not found" {
				sendLog("Error while getting UniventionPerson to updste: " + err.Error() + "Stop programm")
				log.Println(err)
				return
			}
		}
		var ch = make(chan string, len(updateDatas))
		for _, updateData := range updateDatas {
			log.Println("Update")
			go ucsUpdateUser(setup, updateData, institutionID, ch)
		}

		for i := 0; i < len(updateDatas); i++ {
			log.Println("Log Prozess!")
			//		log.Println(<-ch)
			sendLog(<-ch)
		}
	}
}

func ucsImportUser(syncSetup ucsSyncSetup, person itswizard_basic.UniventionPerson, institutionID uint) (out string) {

	log.Println("Checke ob Person nciht gelöscht werden sollte statt import")
	if strings.Contains(person.Data, `object": null,`) {
		out = out + "Person is to delete"
		log.Println(out)
		person.ToUpdate = false
		person.ToDelete = true
		person.Success = false
		person.Errorstring = ""
		person.Error = false
		person.ToImport = false
		person.UpdateStammschule = false
		person.UdpateFirstName = false
		person.UdpateUsername = false
		person.UdpateProfile = false
		person.UdpateProfile = false
		person.UpdateGruppenMitgliedschaften = false
		person.UpdateSchulmitgliedschaften = false
		person.UpdateEmail = false
		person.UpdateDisable = false

		syncSetup.db.Save(&person)
		return
	}

	log.Println("importiere Person", person.Username)

	schulmitgliedschaften, err := getSchulmitgliedschaften(syncSetup, person)
	if err != nil {
		out = out + "getSchulmitgliedschaften " + err.Error()
		log.Println(err)
		saveImportedPersonWithError(syncSetup, person, err)
		return
	}

	gruppenmitgliedschaften, err := getGruppenmitgliedschaftenWolfsburg(person, syncSetup.db)
	//	gruppenmitgliedschaften, err := getGruppenmitgliedschaften(person)
	if err != nil {
		out = out + "getGruppenmitgliedschaften " + err.Error()
		log.Println(err)
		saveImportedPersonWithError(syncSetup, person, err)
		return
	}

	isPersonToImport := isPersonToImport(syncSetup, person, institutionID, schulmitgliedschaften)
	if !isPersonToImport {
		out = "PERSON IS NOT TO IMPORT"
		log.Println("PERSON IS NOT TO IMPORT")
		saveImportedPersonWithSuccess(syncSetup, person)
		return
	}
	out = out + "Person " + person.Username + " wird importiert von id" + strconv.Itoa(int(institutionID))
	log.Println("Person "+person.Username+" wird importiert von id", institutionID)
	// Person importieren
	resp, err := syncSetup.itsl.CreatePerson(itswizard_basic.DbPerson15{
		SyncPersonKey: person.PersonSyncKey,
		FirstName:     prepareFirstname(syncSetup, person),
		LastName:      prepareLastname(person),
		Username:      person.Username,
		Profile:       prepareProfil(person, makeToAdmin(syncSetup, person)),
		Email:         prepareEmail(syncSetup, person),
	})

	if err != nil {
		out = out + "Create Person with error" + person.Username + err.Error()
		log.Println(err)
		saveImportedPersonWithError(syncSetup, person, errors.New(resp))
		return
	}

	for school, profil := range schulmitgliedschaften {
		if !IsSchoolToImportOuSelect(syncSetup, school, institutionID) {
			break
		}

		err = checkIfSchoolExist(syncSetup, school)
		if err != nil {
			saveImportedPersonWithError(syncSetup, person, errors.New(resp))
			return
		}
		out = out + " importiere Schulmitgliedschaft " + person.Username + " " + school
		log.Println("importiere Schulmitgliedschaft", person.Username, school)
		resp, err := syncSetup.itsl.CreateMembership(school, person.PersonSyncKey, profil)
		if err != nil {
			log.Println(err)
			out = out + "Problem by creating Membership " + school + person.PersonSyncKey + profil
			saveImportedPersonWithError(syncSetup, person, errors.New(resp))
			return
		}
	}

	// 3. Gruppenmitgliedschaften erstellen
	for group, school := range gruppenmitgliedschaften {
		if !IsSchoolToImportOuSelect(syncSetup, school, institutionID) {
			break
		}
		if makeToAdmin(syncSetup, person) {
			break
		}
		err = checkIfGroupExist(syncSetup, group, school)
		if err != nil {
			saveImportedPersonWithError(syncSetup, person, errors.New(resp))
			return
		}

		log.Println("importiere Gruppenmitgliedschaft", person.Username, group, "von id", institutionID)
		out = out + "importiere Gruppenmitgliedschaft " + person.Username + " " + group + " von id " + strconv.Itoa(int(institutionID))

		resp, err := syncSetup.itsl.CreateMembership(group, person.PersonSyncKey, schulmitgliedschaften[school])
		if err != nil {
			saveImportedPersonWithError(syncSetup, person, errors.New(resp))
			return
		}
	}

	saveImportedPersonWithSuccess(syncSetup, person)
	return
}

func ucsDeleteUser(syncSetup ucsSyncSetup, person itswizard_basic.UniventionPerson, insstitutionid uint) (out string) {
	out = out + "Lösche Person " + person.Username + " institutionid " + strconv.Itoa(int(insstitutionid))
	log.Println("Lösche Person", person.Username, "institutionid", insstitutionid)
	out = out + "Checke ob Person wirklich gelöscht werden sollte"
	log.Println("Checke ob Person wirklich gelöscht werden sollte")
	if !strings.Contains(person.Data, `object": null,`) {
		log.Println("Person ist nicht zu löschen, versuche ein update")
		out = out + "Person ist nicht zu löschen, versuche ein update"
		person.ToUpdate = true
		person.ToDelete = false
		person.Success = false
		person.Errorstring = ""
		person.Error = false
		person.ToImport = false
		person.UpdateStammschule = false
		person.UdpateFirstName = true
		person.UdpateUsername = true
		person.UdpateProfile = true
		person.UpdateGruppenMitgliedschaften = true
		person.UpdateSchulmitgliedschaften = true
		person.UpdateEmail = true

		syncSetup.db.Save(&person)
		return
	}
	log.Println("Lösche")
	out = out + "Lösche Nutzer " + person.Username
	resp, err := syncSetup.itsl.DeletePerson(person.PersonSyncKey)
	if err != nil {
		saveDeletedPersonWithError(syncSetup, person, errors.New(resp))
		return
	}
	saveDeletedPersonWithSuccess(syncSetup, person)
	out = out + "fertig gelöscht " + person.Username
	log.Println("fertig gelöscht")
	return
}

func ucsUpdateUser(syncSetup ucsSyncSetup, person itswizard_basic.UniventionPerson, insstitutionid uint, ch chan string) {
	schulmitgliedschaften, err := getSchulmitgliedschaften(syncSetup, person)
	if err != nil {
		log.Println(err)
		saveImportedPersonWithError(syncSetup, person, err)
		ch <- fmt.Sprint(insstitutionid, person.Username, "Fehler bei der Schulmitgliedschaften")
		return
	}

	isPersonToImport := isPersonToImport(syncSetup, person, insstitutionid, schulmitgliedschaften)
	if !isPersonToImport {
		log.Println("PERSON IS NOT TO IMPORT")
		saveImportedPersonWithSuccess(syncSetup, person)
		ch <- fmt.Sprint("PERSON IS NOT TO IMPORT", person.Username, insstitutionid)
		return
	}
	log.Println("Checke ob Person nciht gelöscht werden sollte statt import")
	if strings.Contains(person.Data, `object": null,`) {
		person.ToUpdate = false
		person.ToDelete = true
		person.Success = false
		person.Errorstring = ""
		person.Error = false
		person.ToImport = false
		person.UpdateStammschule = false
		person.UdpateFirstName = false
		person.UdpateUsername = false
		person.UdpateProfile = false
		person.UdpateProfile = false
		person.UpdateGruppenMitgliedschaften = false
		person.UpdateSchulmitgliedschaften = false
		person.UpdateEmail = false
		person.UpdateDisable = false

		syncSetup.db.Save(&person)
		ch <- fmt.Sprint("Person is to delete", person.Username, insstitutionid)
		return
	}
	log.Println("Update Person", person.Username, "institution", insstitutionid)
	if person.Data == "" {
		ch <- fmt.Sprint("No Data inside", person.Username, insstitutionid)
		return
	}

	//Checken ob nur Update ist:
	person.UdpateFirstName = true

	//1. Upoate FirstName
	if person.UdpateFirstName {
		resp, err := syncSetup.itsl.UpdateFirstName(person.PersonSyncKey, prepareFirstname(syncSetup, person))
		if err != nil {
			saveUpdatedPersonWithError(syncSetup, person, errors.New(resp))
			ch <- fmt.Sprint(person.Username, insstitutionid, "Update Firstname", resp)
			return
		}
	}

	//2. Upoate LastName
	if person.UdpateLastName {
		resp, err := syncSetup.itsl.UpdateLastName(person.PersonSyncKey, prepareLastname(person))
		if err != nil {
			saveUpdatedPersonWithError(syncSetup, person, errors.New(resp))
			ch <- fmt.Sprint(person.Username, insstitutionid, "Update Lastname", resp)
			return
		}
	}

	//3. Upoate UserName
	if person.UdpateUsername {
		resp, err := syncSetup.itsl.UpdateUsername(person.PersonSyncKey, person.Username)
		if err != nil {
			saveUpdatedPersonWithError(syncSetup, person, errors.New(resp))
			ch <- fmt.Sprint(person.Username, insstitutionid, "Update Username", resp)
			return
		}
	}

	//4. Update Profile
	if person.UdpateProfile {
		// Person importieren
		resp, err := syncSetup.itsl.CreatePerson(itswizard_basic.DbPerson15{
			SyncPersonKey: person.PersonSyncKey,
			FirstName:     prepareFirstname(syncSetup, person),
			LastName:      prepareLastname(person),
			Username:      person.Username,
			Profile:       prepareProfil(person, makeToAdmin(syncSetup, person)),
			Email:         prepareEmail(syncSetup, person),
		})

		if err != nil {
			saveUpdatedPersonWithError(syncSetup, person, errors.New(resp))
			ch <- fmt.Sprint(person.Username, insstitutionid, "Create Person", resp)
			return
		}
	}

	//5. Update Stammschule
	//TODO Wird in der aktuellen Version nicht unterstützt.

	//6. Update Email
	if person.UpdateEmail {
		resp, err := syncSetup.itsl.UpdateEmail(person.PersonSyncKey, prepareEmail(syncSetup, person))
		if err != nil {
			saveUpdatedPersonWithError(syncSetup, person, errors.New(resp))
			ch <- fmt.Sprint(person.Username, insstitutionid, "Udpate Email", resp)
			return
		}
	}

	//7. Update Schulmitgliedschaften
	if person.UpdateSchulmitgliedschaften || person.UpdateGruppenMitgliedschaften {
		//Alle Schulmitgliedschaften löschen
		r := syncSetup.itsl.ReadMembershipsForPerson(person.PersonSyncKey)
		for _, mem := range r {
			resp, err := syncSetup.itsl.DeleteMembership(mem.ID)
			if err != nil {
				saveUpdatedPersonWithError(syncSetup, person, errors.New(resp))
				ch <- fmt.Sprint(person.Username, insstitutionid, "Update Username", resp)
				return
			}
		}

		schulmitgliedschaften, err := getSchulmitgliedschaften(syncSetup, person)
		if err != nil {
			saveUpdatedPersonWithError(syncSetup, person, err)
			ch <- fmt.Sprint(person.Username, insstitutionid, "Get Schulmitgliedschaften", err)
			return
		}
		log.Println("Schumitgliedschaften:", schulmitgliedschaften)

		/*
			ÄNDERUNG
		*/
		gruppenmitgliedschaften, err := getGruppenmitgliedschaftenWolfsburg(person, syncSetup.db)
		//gruppenmitgliedschaften, err := getGruppenmitgliedschaften(person)

		if err != nil {
			saveUpdatedPersonWithError(syncSetup, person, err)
			ch <- fmt.Sprint(person.Username, insstitutionid, "Get Gruppenmitgliedschaften", err)
			return
		}
		log.Println("gruppenmitgliedschaften:", gruppenmitgliedschaften)

		for school, profil := range schulmitgliedschaften {
			if !IsSchoolToImportOuSelect(syncSetup, school, insstitutionid) {
				continue
			}

			err = checkIfSchoolExist(syncSetup, school)
			if err != nil {
				saveUpdatedPersonWithError(syncSetup, person, err)
				ch <- person.Username + " Check if School exist " + err.Error()
				return
			}

			resp, err := syncSetup.itsl.CreateMembership(school, person.PersonSyncKey, profil)
			if err != nil {
				saveUpdatedPersonWithError(syncSetup, person, errors.New(resp))
				ch <- fmt.Sprint(person.Username, insstitutionid, "CreateMembership", resp)
				return
			}
		}

		for group, school := range gruppenmitgliedschaften {
			if !IsSchoolToImportOuSelect(syncSetup, school, insstitutionid) {
				log.Println("Schule ist nicht zu importieren")
				continue
			}
			if makeToAdmin(syncSetup, person) {
				log.Println("Make to admin")
				break
			}
			log.Println(group)
			err = checkIfGroupExist(syncSetup, group, school)
			if err != nil {
				saveUpdatedPersonWithError(syncSetup, person, err)
				ch <- fmt.Sprint(person.Username, insstitutionid, "checkIfGroupExist", err)
				return
			}

			resp, err := syncSetup.itsl.CreateMembership(group, person.PersonSyncKey, schulmitgliedschaften[school])
			if err != nil {
				saveUpdatedPersonWithError(syncSetup, person, errors.New(resp))
				ch <- fmt.Sprint(person.Username, insstitutionid, "Create Membership", resp)
				return
			}
		}
	}

	/*
		//9. Update Disable
		if person.UpdateDisable {
			//Checken, ob das überhaupt in Frage kommt.
			if syncSetup.UCSSetupSyncDisabled {
				resp,err := syncSetup.itsl.DeletePerson(person.PersonSyncKey)
				if err != nil {
					saveUpdatedPersonWithError(syncSetup,person,errors.New(resp))
					ch <- err.Error()
					return
				}
				syncSetup.db.Save(&itswizard_basic.UcsProtokoll{
					Username:    person.Username,
					UUID:        person.PersonSyncKey,
					Action:      "Disable - Benutzerlöschung",
					Success:     true,
					Errorstring: "",
				})
			}
		}
	*/

	ch <- fmt.Sprint("Person with Name ", person.Username, " from institution ", insstitutionid, " was updated successfully.")
	saveUpdatedPersonWithSuccess(syncSetup, person)
}

// Hilfsfunktionen:
func firstnameToOneLetter(firstname string) string {
	if firstname == "" {
		return "NN"
	}
	x := strings.Fields(firstname)
	for _, v := range x[0] {
		return string(v) + "."
		break
	}
	return "NN"
}

func firstnameToOneName(firstname string) string {
	if firstname == "" {
		return "NN"
	}
	x := strings.Fields(firstname)
	return x[0]
}

func prepareEmail(syncSetup ucsSyncSetup, person itswizard_basic.UniventionPerson) string {
	// EMAIL Bearbeiten //
	email := person.Email
	if syncSetup.UCSSetupEmailNotToSync {
		email = ""
	}
	//ENDE EMAIL
	return email
}

func prepareFirstname(syncSetup ucsSyncSetup, person itswizard_basic.UniventionPerson) string {
	// FIRSTNAME bearbeiten //
	firstName := person.FirstName
	if person.FirstName == "" {
		firstName = "NN"
	}
	if syncSetup.UCSSetupMakeTeacherFirstnameToOneLetter {
		if person.Profile == "Staff" {
			firstName = firstnameToOneLetter(firstName)
		}
		if person.Profile == "Staff" {
			firstName = firstnameToOneLetter(firstName)
		}
	}
	if syncSetup.UCSSetupMakeStudentFirstnameToOneLetter {
		if person.Profile == "Student" {
			firstName = firstnameToOneLetter(firstName)
		}
	}
	if syncSetup.UCSSetupMakeTeacherFirstnameToOneName {
		if person.Profile == "Staff" {
			firstName = firstnameToOneName(firstName)
		}
		if person.Profile == "Administrator" {
			firstName = firstnameToOneName(firstName)
		}
	}
	if syncSetup.UCSSetupMakeStudentFirstnameToOneName {
		if person.Profile == "Student" {
			firstName = firstnameToOneName(firstName)
		}
	}
	for _, personSyncKeyFullFirstname := range syncSetup.UCSSetupPeronFullFirstNames {
		if personSyncKeyFullFirstname == person.PersonSyncKey {
			firstName = person.FirstName
		}
	}
	//ENDE FIRSTNAME
	return firstName
}

func prepareLastname(person itswizard_basic.UniventionPerson) string {
	// LASTNAME bearbeiten
	lastName := person.LastName
	if person.LastName == "" {
		lastName = "NN"
	}
	return lastName
}

func prepareProfil(person itswizard_basic.UniventionPerson, makeToAdmin bool) string {
	// 1. Person erstellen in itslearning
	profile := person.Profile
	if makeToAdmin {
		profile = "Administrator"
	}
	return profile
}

func makeToAdmin(syncSetup ucsSyncSetup, person itswizard_basic.UniventionPerson) bool {
	//Check #Admin im Lastname
	makeToAdmin := false
	if syncSetup.UCSSetupAdminSpecification {
		for _, lastname := range syncSetup.UCSSetupAdminLastNames {
			if person.LastName == lastname {
				makeToAdmin = true
				return makeToAdmin
			}
		}

	}
	return makeToAdmin
	// ENDE LASTNAME

}

func getSchulmitgliedschaften(syncSetup ucsSyncSetup, person itswizard_basic.UniventionPerson) (schulmitgliedschaften map[string]string, err error) {
	// Kontrolle nach #Admin
	schulmitgliedschaften = make(map[string]string)
	err = json.Unmarshal([]byte(person.Schulmitgliedschaften), &schulmitgliedschaften)
	if makeToAdmin(syncSetup, person) {
		for school, _ := range schulmitgliedschaften {
			schulmitgliedschaften[school] = "Administrator"
		}
	}
	return schulmitgliedschaften, err
}

func getGruppenmitgliedschaftenWolfsburg(person itswizard_basic.UniventionPerson, db *gorm.DB) (gruppenmitgliedschaften map[string]string, err error) {
	gruppenmitgliedschaften = make(map[string]string)
	err = json.Unmarshal([]byte(person.GruppenMitgliedschaften), &gruppenmitgliedschaften)
	if err != nil {
		return nil, err
	}

	if person.Profile == "Staff" {
		var teacherGroupNames []itswizard_basic.UcsTeacherGroupName
		err = db.Find(&teacherGroupNames).Error
		if len(teacherGroupNames) == 0 {
			return gruppenmitgliedschaften, nil
		}
		if err != nil {
			fmt.Println(err)
			return gruppenmitgliedschaften, err
		}

		gruppenmitgliedschaftenNeu := make(map[string]string)

		for gruppe, schule := range gruppenmitgliedschaften {
			for _, v := range teacherGroupNames {
				if strings.Contains(gruppe, v.Name) {
					gruppenmitgliedschaftenNeu[gruppe] = schule
				}
			}
		}
		gruppenmitgliedschaften = gruppenmitgliedschaftenNeu
	}

	return gruppenmitgliedschaften, err
}

func isPersonToImport(syncSetup ucsSyncSetup, person itswizard_basic.UniventionPerson, institution_id uint, schulmitgliedschaften map[string]string) bool {

	var isPersonToImport bool
	isPersonToImport = false

	if syncSetup.UCSSetupSyncDisabled {
		if person.Disabled {
			log.Println("Person is not to import")
			return isPersonToImport
		}
	}

	var univentionservice itswizard_basic.UniventionService
	err := syncSetup.dbClient.Where("insitution_id = ?", institution_id).Last(&univentionservice).Error
	if err != nil {
		log.Println(err)
	}

	ousMap := make(map[string]bool)

	if univentionservice.SelectOrganisations {
		var ous []itswizard_basic.UniventionOrganisationSelect
		log.Println("Es muss ein OUSelect untersucht werden")
		err := syncSetup.dbClient.Where("institution_id = ?", institution_id).Find(&ous).Error
		if err != nil {
			log.Println(err)
		}
		for _, v := range ous {
			ousMap[v.OUName] = true
		}
	}

	if univentionservice.SelectOrganisations {
		for school, _ := range schulmitgliedschaften {
			if ousMap[school] {
				isPersonToImport = true
			}
		}
	} else {
		isPersonToImport = true
	}
	log.Println("Is to import:", isPersonToImport)
	return isPersonToImport
}

func isPersonToImport2(syncSetup ucsSyncSetup, person itswizard_basic.UniventionPerson, schulmitgliedschaften map[string]string) bool {
	var isPersonToImport bool
	isPersonToImport = true
	ouSelectMap := make(map[string]bool)
	for _, o := range syncSetup.Ous {
		ouSelectMap[o] = true
	}
	log.Println(syncSetup.Ous)
	log.Println("Select", syncSetup.OUSelect)
	if syncSetup.OUSelect {
		for school, _ := range schulmitgliedschaften {
			log.Println("--", school, "--")
			log.Println(ouSelectMap[school])
			log.Println("--", school, "--")
			isPersonToImport = false
			if ouSelectMap[school] {
				isPersonToImport = true
			}
		}
	}

	// Check: Ob der Nutzer Disable
	if syncSetup.UCSSetupSyncDisabled {
		if person.Disabled {
			isPersonToImport = false
		}
	}
	// Ende Check Nutzer Disable
	log.Println(isPersonToImport)
	return isPersonToImport
}

func IsSchoolToImportOuSelect2(syncSetup ucsSyncSetup, school string) bool {
	log.Println("All schools to sync:", syncSetup.Ous)
	log.Println("School to check:", syncSetup.Ous)
	if !syncSetup.OUSelect {
		log.Println("True, no OUSelect")
		return true
	}
	if syncSetup.OUSelect {
		for _, schoolsToSync := range syncSetup.Ous {
			if schoolsToSync == school {
				log.Println("True")
				return true
			}
		}
	}
	log.Println("False")
	return false
}

func IsSchoolToImportOuSelect(syncSetup ucsSyncSetup, school string, institution_id uint) bool {
	log.Println("Prüfen einer Schule")

	var isToImport bool
	isToImport = false

	var univentionservice itswizard_basic.UniventionService
	err := syncSetup.dbClient.Where("insitution_id = ?", institution_id).Last(&univentionservice).Error
	if err != nil {
		log.Println(err)
	}

	ousMap := make(map[string]bool)

	if univentionservice.SelectOrganisations {
		var ous []itswizard_basic.UniventionOrganisationSelect
		log.Println("Es muss ein OUSelect untersucht werden")
		err := syncSetup.dbClient.Where("institution_id = ?", institution_id).Find(&ous).Error
		if err != nil {
			log.Println(err)
		}
		for _, v := range ous {
			ousMap[v.OUName] = true
		}
	}

	if univentionservice.SelectOrganisations {
		if ousMap[school] {
			isToImport = true
		}
	} else {
		isToImport = true
	}
	log.Println("School is to import: ", isToImport)
	return isToImport

}

func checkIfSchoolExist(syncSetup ucsSyncSetup, school string) error {
	//Check if School exist
	if syncSetup.itsl.ReadGroup(school).Group.Name == "" {
		resp, err := syncSetup.itsl.CreateGroup(itswizard_basic.DbGroup15{
			SyncID:        school,
			Name:          school,
			ParentGroupID: "0",
			Level:         0,
		}, true)
		if err != nil {
			return errors.New(resp)
		}
	}
	return nil
}

func checkIfGroupExist(syncSetup ucsSyncSetup, group, school string) error {
	if syncSetup.itsl.ReadGroup(group).Group.Name == "" {
		resp, err := syncSetup.itsl.CreateGroup(itswizard_basic.DbGroup15{
			SyncID:        group,
			Name:          group,
			ParentGroupID: school,
			Level:         1,
		}, false)
		if err != nil {
			return errors.New(resp)
		}
	}
	return nil
}

//Speicherung der Datenbankportationen:

func saveImportedPersonWithError(syncSetup ucsSyncSetup, person itswizard_basic.UniventionPerson, err error) {
	syncSetup.db.Save(&itswizard_basic.UcsProtokoll{
		Username:    person.Username,
		UUID:        person.PersonSyncKey,
		Action:      "Benutzerimport",
		Success:     false,
		Errorstring: err.Error(),
	})

	person.ToUpdate = false
	person.ToDelete = false
	person.Success = false
	person.Errorstring = err.Error()
	person.Error = true
	person.ToImport = true
	person.UpdateStammschule = false
	person.UdpateFirstName = false
	person.UdpateUsername = false
	person.UdpateProfile = false
	person.UdpateProfile = false
	person.UpdateGruppenMitgliedschaften = false
	person.UpdateSchulmitgliedschaften = false
	person.UpdateEmail = false
	person.UpdateDisable = false

	syncSetup.db.Save(&person)
}

func saveImportedPersonWithSuccess(syncSetup ucsSyncSetup, person itswizard_basic.UniventionPerson) {
	syncSetup.db.Save(&itswizard_basic.UcsProtokoll{
		Username:    person.Username,
		UUID:        person.PersonSyncKey,
		Action:      "Benutzerimport",
		Success:     true,
		Errorstring: "",
	})

	person.ToUpdate = false
	person.ToDelete = false
	person.Success = true
	person.Errorstring = ""
	person.Error = false
	person.ToImport = false
	person.UpdateStammschule = false
	person.UdpateFirstName = false
	person.UdpateUsername = false
	person.UdpateProfile = false
	person.UdpateProfile = false
	person.UpdateGruppenMitgliedschaften = false
	person.UpdateSchulmitgliedschaften = false
	person.UpdateEmail = false
	person.UpdateDisable = false

	syncSetup.db.Save(&person)
}

func saveUpdatedPersonWithError(syncSetup ucsSyncSetup, person itswizard_basic.UniventionPerson, err error) {
	syncSetup.db.Save(&itswizard_basic.UcsProtokoll{
		Username:    person.Username,
		UUID:        person.PersonSyncKey,
		Action:      "Benutzerupdate",
		Success:     false,
		Errorstring: err.Error(),
	})

	person.ToUpdate = true
	person.ToDelete = false
	person.Success = false
	person.Errorstring = err.Error()
	person.Error = true
	person.ToImport = false
	person.UpdateStammschule = false
	person.UdpateFirstName = false
	person.UdpateUsername = false
	person.UdpateProfile = false
	person.UdpateProfile = false
	person.UpdateGruppenMitgliedschaften = false
	person.UpdateSchulmitgliedschaften = false
	person.UpdateEmail = false
	person.UpdateDisable = false

	syncSetup.db.Save(&person)
}

func saveUpdatedPersonWithSuccess(syncSetup ucsSyncSetup, person itswizard_basic.UniventionPerson) {
	syncSetup.db.Save(&itswizard_basic.UcsProtokoll{
		Username:    person.Username,
		UUID:        person.PersonSyncKey,
		Action:      "Benutzerupdate",
		Success:     true,
		Errorstring: "",
	})

	person.ToUpdate = false
	person.ToDelete = false
	person.Success = true
	person.Errorstring = ""
	person.Error = false
	person.ToImport = false
	person.UpdateStammschule = false
	person.UdpateFirstName = false
	person.UdpateUsername = false
	person.UdpateProfile = false
	person.UdpateProfile = false
	person.UpdateGruppenMitgliedschaften = false
	person.UpdateSchulmitgliedschaften = false
	person.UpdateEmail = false
	person.UpdateDisable = false

	syncSetup.db.Save(&person)
}

func saveDeletedPersonWithError(syncSetup ucsSyncSetup, person itswizard_basic.UniventionPerson, err error) {
	syncSetup.db.Save(&itswizard_basic.UcsProtokoll{
		Username:    person.Username,
		UUID:        person.PersonSyncKey,
		Action:      "Benutzerlöschung",
		Success:     false,
		Errorstring: err.Error(),
	})

	person.ToUpdate = false
	person.ToDelete = true
	person.Success = false
	person.Errorstring = err.Error()
	person.Error = true
	person.ToImport = false
	person.UpdateStammschule = false
	person.UdpateFirstName = false
	person.UdpateUsername = false
	person.UdpateProfile = false
	person.UdpateProfile = false
	person.UpdateGruppenMitgliedschaften = false
	person.UpdateSchulmitgliedschaften = false
	person.UpdateEmail = false
	person.UpdateDisable = false

	syncSetup.db.Save(&person)
}

func saveDeletedPersonWithSuccess(syncSetup ucsSyncSetup, person itswizard_basic.UniventionPerson) {
	syncSetup.db.Save(&itswizard_basic.UcsProtokoll{
		Username:    person.Username,
		UUID:        person.PersonSyncKey,
		Action:      "Benutzerlöschung",
		Success:     true,
		Errorstring: "",
	})

	person.ToUpdate = false
	person.ToDelete = true
	person.Success = true
	person.Errorstring = ""
	person.Error = false
	person.ToImport = false
	person.UpdateStammschule = false
	person.UdpateFirstName = false
	person.UdpateUsername = false
	person.UdpateProfile = false
	person.UdpateProfile = false
	person.UpdateGruppenMitgliedschaften = false
	person.UpdateSchulmitgliedschaften = false
	person.UpdateEmail = false
	person.UpdateDisable = false

	syncSetup.db.Save(&person)
}

type RunningService struct {
	gorm.Model
	ServiceName string
	LastRun     string
}
