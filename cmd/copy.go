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
	"log"

	"github.com/icemobilelab/amqp-go-tool/internal/pkg/amqpcmds"
	"github.com/spf13/cobra"
)

// moveCmd represents the move command
var copyCmd = &cobra.Command{
	Use:   "copy [origin_queue] [destiny_queue]",
	Short: "Copy messages from one queue to another one",
	Long: `Copy messages from one queue to another one.

The messages processed are also written in a external file (or stdout
if file is not specified).  `,

	Args: cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		src := args[0]
		dst := args[1]
		ci := amqpcmds.CommandInfo{
			User:            username,
			Password:        password,
			Host:            host,
			Port:            port,
			AutoACK:         false,
			Prefetch:        prefetch,
			Count:           count,
			File:            file,
			FormatPrefix:    formatPrefix,
			FormatSeparator: formatSeparator,
			FormatPostfix:   formatPostfix,
		}
		err := ci.CommandCopyMoveToQueue(src, dst)
		if err != nil {
			log.Fatal(err)
		}
	},
}

func init() {
	rootCmd.AddCommand(copyCmd)

	copyCmd.Flags().StringVar(&file, "file", "", "Output file for messages (no value for stdout)")
	copyCmd.Flags().IntVar(&count, "count", 0, "Messages to export (0 for keep waiting for messages)")
	copyCmd.Flags().IntVar(&prefetch, "prefetch", 1, "Prefetch value to consumer messages")
	copyCmd.Flags().StringVar(&formatPrefix, "formatPrefix", "", "Prefix value for the message list")
	copyCmd.Flags().StringVar(&formatSeparator, "formatSeparator", "\n", "Separator between messages")
	copyCmd.Flags().StringVar(&formatPostfix, "formatPostfix", "", "Post-fix value for the message list")
}
