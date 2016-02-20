#!/usr/bin/env ruby
# %RUBY sar data import

# usage
# bundle exec ./sardata_importer.rb data_file config_file

require "csv"
require "pp"
require "yaml"
require "pg"

module SardataImporter
  class Stocker
    
    DEFINITIONS = [
      {
        :type => "cpu", :dev_name => "cpu_no", :fields => {
          "%user"   => { :col_name => "per_user",   :data_type => :numeric, :precision => 5, :scale => 2 },
          "%usr"    => { :col_name => "per_usr",    :data_type => :numeric, :precision => 5, :scale => 2 },
          "%nice"   => { :col_name => "per_nice",   :data_type => :numeric, :precision => 5, :scale => 2 },
          "%system" => { :col_name => "per_system", :data_type => :numeric, :precision => 5, :scale => 2 },
          "%sys"    => { :col_name => "per_sys",    :data_type => :numeric, :precision => 5, :scale => 2 },
          "%iowait" => { :col_name => "per_iowait", :data_type => :numeric, :precision => 5, :scale => 2 },
          "%steal"  => { :col_name => "per_steal",  :data_type => :numeric, :precision => 5, :scale => 2 },
          "%irq"    => { :col_name => "per_irq",    :data_type => :numeric, :precision => 5, :scale => 2 },
          "%soft"   => { :col_name => "per_soft",   :data_type => :numeric, :precision => 5, :scale => 2 },
          "%guest"  => { :col_name => "per_guest",  :data_type => :numeric, :precision => 5, :scale => 2 },
          "%gnice"  => { :col_name => "per_gnice",  :data_type => :numeric, :precision => 5, :scale => 2 },
          "%idle"   => { :col_name => "per_idle",   :data_type => :numeric, :precision => 5, :scale => 2 },
        }
      },
      {
        :type => "pcsw", :fields => {
          "proc/s"  => { :col_name => "procps",  :data_type => :numeric, :precision => 8, :scale => 2 },
          "cswch/s" => { :col_name => "cswchps", :data_type => :numeric, :precision => 8, :scale => 2 },
        }
      },
      {
        :type => "irq", :dev_name => "irq_no", :fields => {
          "intr/s" => { :col_name => "intrps", :data_type => :numeric, :precision => 8, :scale => 2 },
        }
      },
      {
        :type => "swap", :fields => {
          "pswpin/s"  => { :col_name => "pswpinps",  :data_type => :numeric, :precision => 8, :scale => 2 },
          "pswpout/s" => { :col_name => "pswpoutps", :data_type => :numeric, :precision => 8, :scale => 2 },
        }
      },
      {
        :type => "paging", :fields => {
          "pgpgin/s"  => { :col_name => "pgpginps",  :data_type => :numeric, :precision => 8, :scale => 2 },
          "pgpgout/s" => { :col_name => "pgpgoutps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "fault/s"   => { :col_name => "faultps",   :data_type => :numeric, :precision => 8, :scale => 2 },
          "majflt/s"  => { :col_name => "majfltps",  :data_type => :numeric, :precision => 8, :scale => 2 },
          "pgfree/s"  => { :col_name => "pgfreeps",  :data_type => :numeric, :precision => 8, :scale => 2 },
          "pgscank/s" => { :col_name => "pgscankps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "pgscand/s" => { :col_name => "pgscandps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "pgsteal/s" => { :col_name => "pgstealps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "%vmeff"    => { :col_name => "per_vmeff", :data_type => :numeric, :precision => 8, :scale => 2 },
        }
      },
      {
        :type => "io", :fields => {
          "tps"     => { :col_name => "tps",     :data_type => :numeric, :precision => 8, :scale => 2 },
          "rtps"    => { :col_name => "rtps",    :data_type => :numeric, :precision => 8, :scale => 2 },
          "wtps"    => { :col_name => "wtps",    :data_type => :numeric, :precision => 8, :scale => 2 },
          "bread/s" => { :col_name => "breadps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "bwrtn/s" => { :col_name => "bwrtnps", :data_type => :numeric, :precision => 8, :scale => 2 },
        }
      },
      {
        :type => "memory", :fields => {
          "frmpg/s"   => { :col_name => "frmpgps",     :data_type => :numeric, :precision => 8, :scale => 2 },
          "bufpg/s"   => { :col_name => "bufpgps",     :data_type => :numeric, :precision => 8, :scale => 2 },
          "campg/s"   => { :col_name => "campgps",     :data_type => :numeric, :precision => 8, :scale => 2 },
          "kbmemfree" => { :col_name => "kbmemfree",   :data_type => :long },
          "kbmemused" => { :col_name => "kbmemused",   :data_type => :long },
          "%memused"  => { :col_name => "per_memused", :data_type => :numeric, :precision => 5, :scale => 2 },
          "kbbuffers" => { :col_name => "kbbuffers",   :data_type => :long },
          "kbcached"  => { :col_name => "kbcached",    :data_type => :long },
          "kbcommit"  => { :col_name => "kbcommit",    :data_type => :long },
          "%commit"   => { :col_name => "per_commit",  :data_type => :numeric, :precision => 6, :scale => 2 },
          "kbactive"  => { :col_name => "kbactive",    :data_type => :long },
          "kbinact"   => { :col_name => "kbinact",     :data_type => :long },
          "kbdirty"   => { :col_name => "kbdirty",     :data_type => :long },
          "kbswpfree" => { :col_name => "kbswpfree",   :data_type => :long },
          "kbswpused" => { :col_name => "kbswpused",   :data_type => :long },
          "%swpused"  => { :col_name => "per_swpused", :data_type => :numeric, :precision => 5, :scale => 2 },
          "kbswpcad"  => { :col_name => "kbswpcad",    :data_type => :long },
          "%swpcad"   => { :col_name => "per_swpcad",  :data_type => :numeric, :precision => 5, :scale => 2 },
        }
      },
      {
        :type => "ktables", :fields => {
          "dentunusd" => { :col_name => "dentunusd", :data_type => :long },
          "file-nr"   => { :col_name => "file_nr",   :data_type => :long },
          "inode-nr"  => { :col_name => "inode_nr",  :data_type => :long },
          "pty-nr"    => { :col_name => "pty_nr",    :data_type => :long },
        }
      },
      {
        :type => "queue", :fields => {
          "runq-sz"  => { :col_name => "runq_sz",  :data_type => :long },
          "plist-sz" => { :col_name => "plist_sz", :data_type => :long },
          "ldavg-1"  => { :col_name => "ldavg_1",  :data_type => :numeric, :precision => 8, :scale => 2 },
          "ldavg-5"  => { :col_name => "ldavg_5",  :data_type => :numeric, :precision => 8, :scale => 2 },
          "ldavg-15" => { :col_name => "ldavg_15", :data_type => :numeric, :precision => 8, :scale => 2 },
          "blocked"  => { :col_name => "blocked",  :data_type => :long },
        }
      },
      {
        :type => "serial", :dev_name => "tty_name", :fields => {
          "rcvin/s"   => { :col_name => "rcvinps",   :data_type => :numeric, :precision => 8, :scale => 2 },
          "xmtin/s"   => { :col_name => "xmtinps",   :data_type => :numeric, :precision => 8, :scale => 2 },
          "framerr/s" => { :col_name => "framerrps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "prtyerr/s" => { :col_name => "prtyerrps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "brk/s"     => { :col_name => "brkps",     :data_type => :numeric, :precision => 8, :scale => 2 },
          "ovrun/s"   => { :col_name => "ovrunps",   :data_type => :numeric, :precision => 8, :scale => 2 },
        }
      },
      {
        :type => "disk", :dev_name => "dev", :fields => {
          "tps"      => { :col_name => "tps",      :data_type => :numeric, :precision => 8, :scale => 2 },
          "rd_sec/s" => { :col_name => "rd_secps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "wr_sec/s" => { :col_name => "wr_secps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "avgrq-sz" => { :col_name => "avgrq_sz", :data_type => :numeric, :precision => 8, :scale => 2 },
          "avgqu-sz" => { :col_name => "avgqu_sz", :data_type => :numeric, :precision => 8, :scale => 2 },
          "await"    => { :col_name => "await",    :data_type => :numeric, :precision => 8, :scale => 2 },
          "svctm"    => { :col_name => "svctm",    :data_type => :numeric, :precision => 8, :scale => 2 },
          "%util"    => { :col_name => "per_util", :data_type => :numeric, :precision => 8, :scale => 2 },
        }
      },
      {
        :type => "net_dev", :dev_name => "iface_name", :fields => {
          "rxpck/s"  => { :col_name => "rxpckps",    :data_type => :numeric, :precision => 8, :scale => 2 },
          "txpck/s"  => { :col_name => "txpckps",    :data_type => :numeric, :precision => 8, :scale => 2 },
          "rxkB/s"   => { :col_name => "rxkbps",     :data_type => :numeric, :precision => 8, :scale => 2 },
          "txkB/s"   => { :col_name => "txkbps",     :data_type => :numeric, :precision => 8, :scale => 2 },
          "rxcmp/s"  => { :col_name => "rxcmpps",    :data_type => :numeric, :precision => 8, :scale => 2 },
          "txcmp/s"  => { :col_name => "txcmpps",    :data_type => :numeric, :precision => 8, :scale => 2 },
          "rxmcst/s" => { :col_name => "rxmcstps",   :data_type => :numeric, :precision => 8, :scale => 2 },
          "%ifutil"  => { :col_name => "per_ifutil", :data_type => :numeric, :precision => 8, :scale => 2 },
        }
      },
      {
        :type => "net_edev", :fields => {
          "rxerr/s"  => { :col_name => "rxerrps",  :data_type => :numeric, :precision => 8, :scale => 2 },
          "txerr/s"  => { :col_name => "txerrps",  :data_type => :numeric, :precision => 8, :scale => 2 },
          "coll/s"   => { :col_name => "collps",   :data_type => :numeric, :precision => 8, :scale => 2 },
          "rxdrop/s" => { :col_name => "rxdropps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "txdrop/s" => { :col_name => "txdropps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "txcarr/s" => { :col_name => "txcarrps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "rxfram/s" => { :col_name => "rxframps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "rxfifo/s" => { :col_name => "rxfifops", :data_type => :numeric, :precision => 8, :scale => 2 },
          "txfifo/s" => { :col_name => "txfifops", :data_type => :numeric, :precision => 8, :scale => 2 },
        }
      },
      {
        :type => "net_nfs", :fields => {
          "call/s"    => { :col_name => "callps",    :data_type => :numeric, :precision => 8, :scale => 2 },
          "retrans/s" => { :col_name => "retransps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "read/s"    => { :col_name => "readps",    :data_type => :numeric, :precision => 8, :scale => 2 },
          "write/s"   => { :col_name => "writeps",   :data_type => :numeric, :precision => 8, :scale => 2 },
          "access/s"  => { :col_name => "accessps",  :data_type => :numeric, :precision => 8, :scale => 2 },
          "getatt/s"  => { :col_name => "getattps",  :data_type => :numeric, :precision => 8, :scale => 2 },
        }
      },
      {
        :type => "net_nfsd", :fields => {
          "scall/s"   => {:col_name => "scallps",   :data_type => :numeric, :precision => 8, :scale => 2 },
          "badcall/s" => {:col_name => "badcallps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "packet/s"  => {:col_name => "packetps",  :data_type => :numeric, :precision => 8, :scale => 2 },
          "udp/s"     => {:col_name => "udpps",     :data_type => :numeric, :precision => 8, :scale => 2 },
          "tcp/s"     => {:col_name => "tcpps",     :data_type => :numeric, :precision => 8, :scale => 2 },
          "hit/s"     => {:col_name => "hitps",     :data_type => :numeric, :precision => 8, :scale => 2 },
          "miss/s"    => {:col_name => "missps",    :data_type => :numeric, :precision => 8, :scale => 2 },
          "sread/s"   => {:col_name => "sreadps",   :data_type => :numeric, :precision => 8, :scale => 2 },
          "swrite/s"  => {:col_name => "swriteps",  :data_type => :numeric, :precision => 8, :scale => 2 },
          "saccess/s" => {:col_name => "saccessps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "sgetatt/s" => {:col_name => "sgetattps", :data_type => :numeric, :precision => 8, :scale => 2 },
        }
      },
      {
        :type => "net_sock", :fields => {
          "totsck"  => { :col_name => "totsck",  :data_type => :long },
          "tcpsck"  => { :col_name => "tcpsck",  :data_type => :long },
          "udpsck"  => { :col_name => "udpsck",  :data_type => :long },
          "rawsck"  => { :col_name => "rawsck",  :data_type => :long },
          "ip-frag" => { :col_name => "ip_frag", :data_type => :long },
          "tcp-tw"  => { :col_name => "tcp_tw",  :data_type => :long },
        }
      },
      {
        :type => "net_ip", :fields => {
          "irec/s"    => { :col_name => "irecps"   , :data_type => :numeric, :precision => 8, :scale => 2 },
          "fwddgm/s"  => { :col_name => "fwddgmps" , :data_type => :numeric, :precision => 8, :scale => 2 },
          "idel/s"    => { :col_name => "idelps"   , :data_type => :numeric, :precision => 8, :scale => 2 },
          "orq/s"     => { :col_name => "orqps"    , :data_type => :numeric, :precision => 8, :scale => 2 },
          "asmrq/s"   => { :col_name => "asmrqps"  , :data_type => :numeric, :precision => 8, :scale => 2 },
          "asmok/s"   => { :col_name => "asmokps"  , :data_type => :numeric, :precision => 8, :scale => 2 },
          "fragok/s"  => { :col_name => "fragokps" , :data_type => :numeric, :precision => 8, :scale => 2 },
          "fragcrt/s" => { :col_name => "fragcrtps", :data_type => :numeric, :precision => 8, :scale => 2 },
        }
      },
      {
        :type => "net_eip", :fields => {
          "ihdrerr/s" => { :col_name => "ihdrerrps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "iadrerr/s" => { :col_name => "iadrerrps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "iukwnpr/s" => { :col_name => "iukwnprps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "idisc/s"   => { :col_name => "idiscps"  , :data_type => :numeric, :precision => 8, :scale => 2 },
          "odisc/s"   => { :col_name => "odiscps"  , :data_type => :numeric, :precision => 8, :scale => 2 },
          "onort/s"   => { :col_name => "onortps"  , :data_type => :numeric, :precision => 8, :scale => 2 },
          "asmf/s"    => { :col_name => "asmfps"   , :data_type => :numeric, :precision => 8, :scale => 2 },
          "fragf/s"   => { :col_name => "fragfps"  , :data_type => :numeric, :precision => 8, :scale => 2 },
        }
      },
      {
        :type => "net_icmp", :fields => {
          "imsg/s"    => { :col_name => "imsgps"   , :data_type => :numeric, :precision => 8, :scale => 2 },
          "omsg/s"    => { :col_name => "omsgps"   , :data_type => :numeric, :precision => 8, :scale => 2 },
          "iech/s"    => { :col_name => "iechps"   , :data_type => :numeric, :precision => 8, :scale => 2 },
          "iechr/s"   => { :col_name => "iechrps"  , :data_type => :numeric, :precision => 8, :scale => 2 },
          "oech/s"    => { :col_name => "oechps"   , :data_type => :numeric, :precision => 8, :scale => 2 },
          "oechr/s"   => { :col_name => "oechrps"  , :data_type => :numeric, :precision => 8, :scale => 2 },
          "itm/s"     => { :col_name => "itmps"    , :data_type => :numeric, :precision => 8, :scale => 2 },
          "itmr/s"    => { :col_name => "itmrps"   , :data_type => :numeric, :precision => 8, :scale => 2 },
          "otm/s"     => { :col_name => "otmps"    , :data_type => :numeric, :precision => 8, :scale => 2 },
          "otmr/s"    => { :col_name => "otmrps"   , :data_type => :numeric, :precision => 8, :scale => 2 },
          "iadrmk/s"  => { :col_name => "iadrmkps" , :data_type => :numeric, :precision => 8, :scale => 2 },
          "iadrmkr/s" => { :col_name => "iadrmkrps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "oadrmk/s"  => { :col_name => "oadrmkps" , :data_type => :numeric, :precision => 8, :scale => 2 },
          "oadrmkr/s" => { :col_name => "oadrmkrps", :data_type => :numeric, :precision => 8, :scale => 2 },
        }
      },
      {
        :type => "net_eicmp", :fields => {
          "ierr/s"    => { :col_name => "ierrps"   , :data_type => :numeric, :precision => 8, :scale => 2 },
          "oerr/s"    => { :col_name => "oerrps"   , :data_type => :numeric, :precision => 8, :scale => 2 },
          "idstunr/s" => { :col_name => "idstunrps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "odstunr/s" => { :col_name => "odstunrps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "itmex/s"   => { :col_name => "itmexps"  , :data_type => :numeric, :precision => 8, :scale => 2 },
          "otmex/s"   => { :col_name => "otmexps"  , :data_type => :numeric, :precision => 8, :scale => 2 },
          "iparmpb/s" => { :col_name => "iparmpbps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "oparmpb/s" => { :col_name => "oparmpbps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "isrcq/s"   => { :col_name => "isrcqps"  , :data_type => :numeric, :precision => 8, :scale => 2 },
          "osrcq/s"   => { :col_name => "osrcqps"  , :data_type => :numeric, :precision => 8, :scale => 2 },
          "iredir/s"  => { :col_name => "iredirps" , :data_type => :numeric, :precision => 8, :scale => 2 },
          "oredir/s"  => { :col_name => "oredirps" , :data_type => :numeric, :precision => 8, :scale => 2 },
        }
      },
      {
        :type => "net_tcp", :fields => {
          "active/s"  => { :col_name => "activeps" , :data_type => :numeric, :precision => 8, :scale => 2 },
          "passive/s" => { :col_name => "passiveps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "iseg/s"    => { :col_name => "isegps"   , :data_type => :numeric, :precision => 8, :scale => 2 },
          "oseg/s"    => { :col_name => "osegps"   , :data_type => :numeric, :precision => 8, :scale => 2 },
        }
      },
      {
        :type => "net_etcp", :fields => {
          "atmptf/s"  => { :col_name => "atmptfps" , :data_type => :numeric, :precision => 8, :scale => 2 },
          "estres/s"  => { :col_name => "estresps" , :data_type => :numeric, :precision => 8, :scale => 2 },
          "retrans/s" => { :col_name => "retransps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "isegerr/s" => { :col_name => "isegerrps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "orsts/s"   => { :col_name => "orstsps"  , :data_type => :numeric, :precision => 8, :scale => 2 },
        }
      },
      {
        :type => "net_udp", :fields => {
          "idgm/s"    => { :col_name => "idgmps"   , :data_type => :numeric, :precision => 8, :scale => 2 },
          "odgm/s"    => { :col_name => "odgmps"   , :data_type => :numeric, :precision => 8, :scale => 2 },
          "noport/s"  => { :col_name => "noportps" , :data_type => :numeric, :precision => 8, :scale => 2 },
          "idgmerr/s" => { :col_name => "idgmerrps", :data_type => :numeric, :precision => 8, :scale => 2 },
        }
      },
      {
        :type => "net_sock6", :fields => {
          "tcp6sck"  => { :col_name => "tcp6sck" , :data_type => :long },
          "udp6sck"  => { :col_name => "udp6sck" , :data_type => :long },
          "raw6sck"  => { :col_name => "raw6sck" , :data_type => :long },
          "ip6-frag" => { :col_name => "ip6_frag", :data_type => :long },
        }
      },
      {
        :type => "net_ip6", :fields => {
          "irec6/s"   => { :col_name => "irec6ps"  , :data_type => :numeric, :precision => 8, :scale => 2 },
          "fwddgm6/s" => { :col_name => "fwddgm6ps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "idel6/s"   => { :col_name => "idel6ps"  , :data_type => :numeric, :precision => 8, :scale => 2 },
          "orq6/s"    => { :col_name => "orq6ps"   , :data_type => :numeric, :precision => 8, :scale => 2 },
          "asmrq6/s"  => { :col_name => "asmrq6ps" , :data_type => :numeric, :precision => 8, :scale => 2 },
          "asmok6/s"  => { :col_name => "asmok6ps" , :data_type => :numeric, :precision => 8, :scale => 2 },
          "imcpck6/s" => { :col_name => "imcpck6ps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "omcpck6/s" => { :col_name => "omcpck6ps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "fragok6/s" => { :col_name => "fragok6ps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "fragcr6/s" => { :col_name => "fragcr6ps", :data_type => :numeric, :precision => 8, :scale => 2 },
        }
      },
      {
        :type => "net_eip6", :fields => {
          "ihdrer6/s" => { :col_name => "idrer6ps" , :data_type => :numeric, :precision => 8, :scale => 2 },
          "iadrer6/s" => { :col_name => "iadrer6ps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "iukwnp6/s" => { :col_name => "iukwnp6ps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "i2big6/s"  => { :col_name => "i2big6ps" , :data_type => :numeric, :precision => 8, :scale => 2 },
          "idisc6/s"  => { :col_name => "idisc6ps" , :data_type => :numeric, :precision => 8, :scale => 2 },
          "odisc6/s"  => { :col_name => "odisc6ps" , :data_type => :numeric, :precision => 8, :scale => 2 },
          "inort6/s"  => { :col_name => "inort6ps" , :data_type => :numeric, :precision => 8, :scale => 2 },
          "onort6/s"  => { :col_name => "onort6ps" , :data_type => :numeric, :precision => 8, :scale => 2 },
          "asmf6/s"   => { :col_name => "asmf6ps"  , :data_type => :numeric, :precision => 8, :scale => 2 },
          "fragf6/s"  => { :col_name => "fragf6ps" , :data_type => :numeric, :precision => 8, :scale => 2 },
          "itrpck6/s" => { :col_name => "itrpck6ps", :data_type => :numeric, :precision => 8, :scale => 2 },
        }
      },
      {
        :type => "net_icmp6", :fields => {
          "imsg6/s"   => { :col_name => "imsg6ps"  , :data_type => :numeric, :precision => 8, :scale => 2 },
          "omsg6/s"   => { :col_name => "omsg6ps"  , :data_type => :numeric, :precision => 8, :scale => 2 },
          "iech6/s"   => { :col_name => "iech6ps"  , :data_type => :numeric, :precision => 8, :scale => 2 },
          "iechr6/s"  => { :col_name => "iechr6ps" , :data_type => :numeric, :precision => 8, :scale => 2 },
          "oechr6/s"  => { :col_name => "oechr6ps" , :data_type => :numeric, :precision => 8, :scale => 2 },
          "igmbq6/s"  => { :col_name => "igmbq6ps" , :data_type => :numeric, :precision => 8, :scale => 2 },
          "igmbr6/s"  => { :col_name => "igmbr6ps" , :data_type => :numeric, :precision => 8, :scale => 2 },
          "ogmbr6/s"  => { :col_name => "ogmbr6ps" , :data_type => :numeric, :precision => 8, :scale => 2 },
          "igmbrd6/s" => { :col_name => "igmbrd6ps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "ogmbrd6/s" => { :col_name => "ogmbrd6ps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "irtsol6/s" => { :col_name => "irtsol6ps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "ortsol6/s" => { :col_name => "ortsol6ps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "irtad6/s"  => { :col_name => "irtad6ps" , :data_type => :numeric, :precision => 8, :scale => 2 },
          "inbsol6/s" => { :col_name => "inbsol6ps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "onbsol6/s" => { :col_name => "onbsol6ps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "inbad6/s"  => { :col_name => "inbad6ps" , :data_type => :numeric, :precision => 8, :scale => 2 },
          "onbad6/s"  => { :col_name => "onbad6ps" , :data_type => :numeric, :precision => 8, :scale => 2 },
        }
      },
      {
        :type => "net_eicmp6", :fields => {
          "ierr6/s"   => { :col_name => "ierr6ps"  , :data_type => :numeric, :precision => 8, :scale => 2 },
          "idtunr6/s" => { :col_name => "idtunr6ps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "odtunr6/s" => { :col_name => "odtunr6ps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "itmex6/s"  => { :col_name => "itmex6ps" , :data_type => :numeric, :precision => 8, :scale => 2 },
          "otmex6/s"  => { :col_name => "otmex6ps" , :data_type => :numeric, :precision => 8, :scale => 2 },
          "iprmpb6/s" => { :col_name => "iprmpb6ps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "oprmpb6/s" => { :col_name => "oprmpb6ps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "iredir6/s" => { :col_name => "iredir6ps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "oredir6/s" => { :col_name => "oredir6ps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "ipck2b6/s" => { :col_name => "ipck2b6ps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "opck2b6/s" => { :col_name => "opck2b6ps", :data_type => :numeric, :precision => 8, :scale => 2 },
        }
      },
      {
        :type => "net_udp6", :fields => {
          "idgm6/s"   => { :col_name => "idgm6ps"  , :data_type => :numeric, :precision => 8, :scale => 2 },
          "odgm6/s"   => { :col_name => "odgm6ps"  , :data_type => :numeric, :precision => 8, :scale => 2 },
          "noport6/s" => { :col_name => "noport6ps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "idgmer6/s" => { :col_name => "idgmer6ps", :data_type => :numeric, :precision => 8, :scale => 2 },
        }
      },
      {
        :type => "pwr_freq", :dev_name => "cpu_no", :fields => {
          "MHz" => { :col_name => "mhz", :data_type => :numeric, :precision => 8, :scale => 2 },
        }
      },
      {
        :type => "pwr_fan", :dev_name => "fan_no", :fields => {
          "rpm"    => { :col_name => "rpm"   , :data_type => :numeric, :precision => 8, :scale => 2 },
          "drpm"   => { :col_name => "drpm"  , :data_type => :numeric, :precision => 8, :scale => 2 },
          "DEVICE" => { :col_name => "device", :data_type => :string },
        }
      },
      {
        :type => "pwr_temp", :dev_name => "temp_no", :fields => {
          "degC"   => { :col_name => "degc"    , :data_type => :numeric, :precision => 8, :scale => 2 },
          "%temp"  => { :col_name => "per_temp", :data_type => :numeric, :precision => 8, :scale => 2 },
          "DEVICE" => { :col_name => "device"  , :data_type => :string },
        }
      },
      {
        :type => "pwr_in", :dev_name => "in_no", :fields => {
          "inV"    => { :col_name => "inv"   , :data_type => :numeric, :precision => 8, :scale => 2 },
          "%in"    => { :col_name => "per_in", :data_type => :numeric, :precision => 8, :scale => 2 },
          "DEVICE" => { :col_name => "device", :data_type => :numeric, :precision => 8, :scale => 2 },
        }
      },
      {
        :type => "huge", :fields => {
          "kbhugfree" => { :col_name => "kbhugfree"  , :data_type => :long },
          "kbhugused" => { :col_name => "kbhugused"  , :data_type => :long },
          "%hugused"  => { :col_name => "per_hugused", :data_type => :numeric, :precision => 5, :scale => 2 },
        }
      },
      {
        :type => "pwr_wghfreq", :dev_name => "cpu_no", :fields => {
          "wghMHz" => { :col_name => "wghmhz", :data_type => :numeric, :precision => 8, :scale => 2 },
        }
      },
      {
        :type => "pwr_usb", :dev_name => "bus_no", :fields => {
          "idvendor" => { :col_name => "idvendor", :data_type => :string },
          "idprod"   => { :col_name => "idprod"  , :data_type => :string },
          "maxpower" => { :col_name => "maxpower", :data_type => :long },
          "manufact" => { :col_name => "manufact", :data_type => :string },
          "product"  => { :col_name => "product" , :data_type => :string },
        }
      },
      {
        :type => "filesystem", :dev_name => "mount", :fields => {
          "MBfsfree" => { :col_name => "mbfsfree"   , :data_type => :long },
          "MBfsused" => { :col_name => "mbfsused"   , :data_type => :long },
          "%fsused"  => { :col_name => "per_fsused" , :data_type => :numeric, :precision => 5, :scale => 2 },
          "%ufsused" => { :col_name => "per_ufsused", :data_type => :numeric, :precision => 5, :scale => 2 },
          "Ifree"    => { :col_name => "ifree"      , :data_type => :long },
          "Iused"    => { :col_name => "iused"      , :data_type => :long },
          "%Iused"   => { :col_name => "per_iused"  , :data_type => :numeric, :precision => 5, :scale => 2 },
        }
      },
      {
        :type => "net_fc", :dev_name => "fchost", :fields => {
          "fch_rxf/s" => { :col_name => "fch_rxfps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "fch_txf/s" => { :col_name => "fch_txfps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "fch_rwx/s" => { :col_name => "fch_rwxps", :data_type => :numeric, :precision => 8, :scale => 2 },
          "fch_txw/s" => { :col_name => "fch_twxps", :data_type => :numeric, :precision => 8, :scale => 2 },
        }
      },
    ]

    INDEX_HOSTNAME = 0
    INDEX_INTERVAL = 1
    INDEX_TIMESTAMP = 2
    INDEX_DEVICE_NAME = 3
    INDEX_FIELD_NAME = 4
    INDEX_VALUE = 5

    FIELD_NAME_HOSTNAME = "hostname"
    FIELD_NAME_TIMESTAMP = "timestamp"
    FIELD_NAME_DEV_NAME = "dev_name"
    
    def self.stockers
      DEFINITIONS.map { |d| Stocker.new(d[:type], d[:dev_name], d[:fields]) }
    end
    
    def initialize(type, dev_name, fields)
      @type = type
      @dev_name = dev_name
      @fields = fields
      @dataset = {}
    end
    
    def acceptable?(row)
      field_name = row[INDEX_FIELD_NAME]
      device_name = row[INDEX_DEVICE_NAME]

      case field_name
      when "tps"
        case @type
        when "io" then (device_name == "-")
        when "disk" then (device_name != "-")
        else false
        end
      when "DEVICE"
        case @type
        when "pwr_fan" then (device_name.match(/fan/))
        when "pwr_temp" then (device_name.match(/temp/))
        when "pwr_in" then (device_name.match(/in/))
        else false
        end
      else
        @fields.keys.include?(field_name)
      end
    end
    
    def add(row)
      @dataset[row[INDEX_HOSTNAME]] ||= {  }
      @dataset[row[INDEX_HOSTNAME]][row[INDEX_TIMESTAMP]] ||= {  }
      @dataset[row[INDEX_HOSTNAME]][row[INDEX_TIMESTAMP]][row[INDEX_DEVICE_NAME]] ||= {  }
      @dataset[row[INDEX_HOSTNAME]][row[INDEX_TIMESTAMP]][row[INDEX_DEVICE_NAME]][row[INDEX_FIELD_NAME]] = row[INDEX_VALUE]
    end

    def stocked_dataset
      result = []
      
      @dataset.keys.each do |hostname|
        ds2 = @dataset[hostname]
        ds2.keys.each do |timestamp|
          ds3 = ds2[timestamp]
          ds3.keys.each do |device_name|
            data = ds3[device_name]
            data[FIELD_NAME_HOSTNAME] = hostname
            data[FIELD_NAME_TIMESTAMP] = timestamp
            data[FIELD_NAME_DEV_NAME] = device_name if @dev_name
            result << data
          end
        end
      end
      
      result
    end

    attr_accessor :type
    attr_accessor :dev_name
    attr_accessor :fields
  end

  module Writer
    class Base
      def self.create(config)
        case config[:mode]
        when "postgresql" then PostgreSQL.new(config)
        else Stdout.new(config)
        end
      end
      
      def initialize(config)
        @hosts = config[:hosts]
      end
      
      def convert_to_ip_addr(hostname)
        @hosts[hostname] || hostname
      end

      def modify_dev_name(dev_name, value)
        case
        when dev_name.nil? then nil
        when dev_name == "cpu_no" then value.gsub(/cpu/, "")
        when dev_name == "irq_no" then value.gsub(/i00/, "").gsub(/i0/, "").gsub(/i/, "")
        when dev_name == "fan_no" then value.gsub(/fan/, "")
        when dev_name == "temp_no" then value.gsub(/temp/, "")
        when dev_name == "in_no" then value.gsub(/in/, "")
        else value
        end
      end
    end
    
    class Stdout < Base
      def initialize(config)
        super(config)
      end
      
      def write(stockers)
        stockers.each do |stocker|
          stocker.stocked_dataset.each do |data|
            puts data
          end
        end
      end
    end

    class PostgreSQL < Base

      COLUMN_NAME_IP_ADDRESS = "ip_addr"
      COLUMN_NAME_TIMESTAMP = "collect_ts"
      
      def initialize(config)
        super(config)
        @datasource = config[:datasource]
      end

      def write(stockers)
        conn = PG::connect(@datasource)
        conn.transaction do |c|
          stockers.each do |stocker|
            table_name = "sar_#{stocker.type}"
            stocker.stocked_dataset.each do |data|
              c.exec(create_sql(stocker, data))
            end
          end
        end
      end

      def create_sql(stocker, data)
        column_names = []
        values = []
        data.keys.each do |field_name|
          value = data[field_name]
          case
          when field_name == SardataImporter::Stocker::FIELD_NAME_HOSTNAME
            column_names << COLUMN_NAME_IP_ADDRESS
            values << "'#{convert_to_ip_addr(value)}'"
            
          when field_name == SardataImporter::Stocker::FIELD_NAME_TIMESTAMP
            column_names << COLUMN_NAME_TIMESTAMP
            values << "TO_TIMESTAMP(#{value})"
            
          when field_name == SardataImporter::Stocker::FIELD_NAME_DEV_NAME
            column_names << stocker.dev_name
            values << "'#{modify_dev_name(stocker.dev_name, value)}'"
            
          when stocker.fields.keys.include?(field_name)
            column_names << stocker.fields[field_name][:col_name]
            values << value
          end
        end
        
        "INSERT INTO sar_#{stocker.type} (#{column_names.join(', ')}) VALUES (#{values.join(', ')});"
      end
    end
  end

  class Main
    def self.main(dataset, writer_config)
      stockers = Stocker.stockers
      
      dataset.each do |data|
        stockers.each do |stocker|
          if stocker.acceptable?(data)
            stocker.add(data)
            break
          end
        end
      end
      
      Writer::Base.create(writer_config).write(stockers)
    end
  end
end

if __FILE__ == $0
  SardataImporter::Main.main(CSV.read(ARGV[0], "r", {:col_sep => "\t"}), YAML.load_file(ARGV[1]))
end
