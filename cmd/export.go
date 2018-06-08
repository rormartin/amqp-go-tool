// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package cmd

import (
	"github.com/icemobilelab/amqp-go-tool/internal/pkg/amqpcmds"
	"github.com/spf13/cobra"
	"log"
)

var (
	autoAck         bool
	prefetch        int
	count           int
	file            string
	formatPrefix    string
	formatSeparator string
	formatPostfix   string
)

// exportCmd represents the export command
var exportCmd = &cobra.Command{
	Use:   "export [queue]",
	Short: "Export the messages from a RabbitMQ queue",
	Long: `Export the messages from a RabbitMQ queue to the stdout or to a file.

Prefix, post-fix and custom message separators are available for
custom formatting.  `,
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		queue := args[0]
		ci := amqpcmds.CommandInfo{
			User:            username,
			Password:        password,
			Host:            host,
			Port:            port,
			AutoACK:         autoAck,
			Prefetch:        prefetch,
			Count:           count,
			File:            file,
			FormatPrefix:    formatPrefix,
			FormatSeparator: formatSeparator,
			FormatPostfix:   formatPostfix,
		}
		err := ci.CommandExport(queue)
		if err != nil {
			log.Fatal(err)
		}
	}}

func init() {
	rootCmd.AddCommand(exportCmd)

	exportCmd.Flags().StringVar(&file, "file", "", "Output file for messages (no value for stdout)")
	exportCmd.Flags().IntVar(&count, "count", 0, "Messages to export (0 for keep waiting for messages)")
	exportCmd.Flags().IntVar(&prefetch, "prefetch", 1, "Prefetch value to consumer messages")
	exportCmd.Flags().BoolVar(&autoAck, "auto-ack", false, "Auto ACK the messages after exported")
	exportCmd.Flags().StringVar(&formatPrefix, "formatPrefix", "", "Prefix value for the message list")
	exportCmd.Flags().StringVar(&formatSeparator, "formatSeparator", "\n", "Separator between messages")
	exportCmd.Flags().StringVar(&formatPostfix, "formatPostfix", "", "Post-fix value for the message list")
}
