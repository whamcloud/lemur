package pdm

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
)

type (

	// HSMConfig is the container for HSM Configuration.
	HSMConfig struct {
		Lustre    string             `json:"lustre"`
		Processes int                `json:"processes"`
		Archives  map[string]Archive `json:"archives"`
	}

	// Archive is the configuration for one backend archive.
	Archive struct {
		Name             string `json:"name"`
		Type             string `json:"type"`
		ArchiveID        uint   `json:"archive_id"`
		S3Url            string `json:"s3_url"`
		PosixDir         string `json:"posix_dir"`
		SnapshotsEnabled bool   `json:"snapshots_enabled"`
	}

	cliArchives []Archive
)

var (
	defaultConfig = HSMConfig{
		Processes: 4,
		Archives:  map[string]Archive{},
	}

	// CLI parameters
	hsmConfig     string
	mnt           string
	processes     int
	archives      cliArchives
	disableMirror bool
	disableConfig bool
)

func (a *cliArchives) String() string {
	return fmt.Sprint(*a)
}

func parseArchiveFlag(value string, newArchive *Archive) error {
	parseError := fmt.Errorf("Unable to parse %s", value)
	fields := strings.Split(value, ":")

	// name:type:number:s3url:posixdir:snapshots
	if len(fields) < 6 {
		return parseError
	}
	for i, item := range fields {
		switch i {
		case 0:
			newArchive.Name = item
		case 1:
			newArchive.Type = item
		case 2:
			if val, err := strconv.Atoi(item); err != nil {
				return err
			} else {
				newArchive.ArchiveID = uint(val)
			}

		case 3:
			newArchive.S3Url = item
		case 4:
			newArchive.PosixDir = item
		case 5:
			if val, err := strconv.ParseBool(item); err != nil {
				return err
			} else {
				newArchive.SnapshotsEnabled = val
			}

		default:
			return parseError
		}
	}

	return nil
}

func (a *cliArchives) Set(value string) error {
	newArchive := Archive{}
	if err := parseArchiveFlag(value, &newArchive); err != nil {
		return err
	}
	*a = append(*a, newArchive)

	return nil
}

const defaultHsmConfig = "/etc/lustre_hsm.conf"

func init() {
	flag.BoolVar(&disableConfig, "disable-config", false, "disable config file")
	flag.BoolVar(&disableMirror, "disable-mirror", false, "disable mirror archive type")
	flag.StringVar(&hsmConfig, "config", defaultHsmConfig, "Lustre HSM config file")
	flag.StringVar(&mnt, "mnt", "", "Lustre mount point.")
	flag.IntVar(&processes, "np", 0, "Number of processes")
	flag.Var(&archives, "archive", "Archive definition(s) (name:type:number:s3url:posixdir:snapshots)")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "usage: %s [--disable-mirror] [--archive <archive>] [--config <file>]  [--mnt <mountpoint>]\n", os.Args[0])
		flag.PrintDefaults()
	}
}

func errExit(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	flag.Usage()
	os.Exit(1)
}

func (conf HSMConfig) String() string {
	b, err := json.Marshal(conf)
	if err != nil {
		log.Fatal(err)
	}
	var out bytes.Buffer
	json.Indent(&out, b, "", "\t")
	return string(out.Bytes())
}

func readConfig(p string, conf *HSMConfig) error {
	data, err := ioutil.ReadFile(p)
	if err != nil {
		return nil
	}
	err = json.Unmarshal(data, conf)
	if err != nil {
		return err
	}
	return nil
}

func ConfigInitMust() *HSMConfig {
	flag.Parse()
	cfg := &defaultConfig

	if !disableConfig {
		err := readConfig(hsmConfig, cfg)
		if err != nil {
			errExit(err.Error())
		}
	}

	if !disableMirror {
		cfg.Archives["mirror"] = Archive{
			Type:      "mirror",
			ArchiveID: 100,
		}
	}

	if len(archives) > 0 {
		for _, archive := range archives {
			cfg.Archives[archive.Name] = archive
		}
	}

	if mnt != "" {
		cfg.Lustre = mnt
	}

	if processes > 0 {
		cfg.Processes = processes
	}

	if cfg.Lustre == "" {
		errExit("! The -mnt <mountpoint> option was not specified and not config.")
	}
	return cfg
}
