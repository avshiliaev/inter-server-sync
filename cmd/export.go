package cmd

import (
	"os"
	"strings"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
	"github.com/uyuni-project/inter-server-sync/entityDumper"
	"github.com/uyuni-project/inter-server-sync/utils"
)

var exportCmd = &cobra.Command{
	Use:   "export",
	Short: "Export server entities to be imported in other server",
	Run:   runExport,
}

var channels []string
var channelWithChildren []string
var outputDir string
var metadataOnly bool
var startingDate string

func init() {
	exportCmd.Flags().StringSliceVar(&channels, "channels", nil, "Channels to be exported")
	exportCmd.Flags().StringSliceVar(&channelWithChildren, "channel-with-children", nil, "Channels to be exported")
	exportCmd.Flags().StringVar(&outputDir, "outputDir", ".", "Location for generated data")
	exportCmd.Flags().BoolVar(&metadataOnly, "metadataOnly", false, "export only metadata")
	exportCmd.Flags().StringVar(&startingDate, "packagesOnlyAfter", "", "Only export packages added or modified after the specified date (date format can be 'YYYY-MM-DD' or 'YYYY-MM-DD hh:mm:ss')")
	exportCmd.Args = cobra.NoArgs

	rootCmd.AddCommand(exportCmd)
}

func runExport(cmd *cobra.Command, args []string) {
	log.Debug().Msg("export called")
	log.Debug().Msg(strings.Join(channels, ","))
	log.Debug().Msg(outputDir)
	// check output dir existance and create it if needed.

	// Validate data
	validatedDate, ok := utils.ValidateDate(startingDate)
	if !ok {
		log.Fatal().Msg("Unable to validate the date. Allowed formats are 'YYYY-MM-DD' or 'YYYY-MM-DD hh:mm:ss'")
	}

	options := entityDumper.ChannelDumperOptions{
		ServerConfig:              serverConfig,
		ChannelLabels:             channels,
		ChannelWithChildrenLabels: channelWithChildren,
		OutputFolder:              outputDir,
		MetadataOnly:              metadataOnly,
		StartingDate:              validatedDate,
	}
	entityDumper.DumpChannelData(options)
	var versionfile string
	versionfile = options.GetOutputFolderAbsPath() + "/version.txt"
	vf, err := os.Open(versionfile)
	defer vf.Close()
	if os.IsNotExist(err) {
		f, err := os.Create(versionfile)
		if err != nil {
			log.Fatal().Msg("Unable to create version file")
		}
		vf = f
	}
	version, product := utils.GetCurrentServerVersion()
	vf.WriteString("product_name = " + product + "\n" + "version = " + version + "\n")
}
