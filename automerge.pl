#!/usr/bin/env perl

# 17 April 2018
# Automatically merge the results databases with the main experiments database.
# Used to analyse the data with Mathematica
#

use strict;
use warnings;
use experimental "switch";

use Array::Utils qw(:all);
use Carp("confess");
use Cwd("abs_path");
use Data::Dumper("Dumper");
use DBI;
use DBI::Const::GetInfoType;
use File::Basename ("dirname");
use File::Copy("cp");
use File::Path("make_path");
use Getopt::Long;
use Sys::Hostname;

# the sqlite database used as destination
our @sources = ();
our $destination;

# the name of the temporary table, to check which records are already present in the main database
our $tbl_temporary = "_TEMP_candidates";

# connection to the destination database
our $conn;
our %conn_tables;

sub print_help(){
    print(<<EOF);
Automatically merge the databases with the experiments from the /tmp folder or from the list of paths given as arguments.
Usage: $0 [-d <destination.sqlite3>] [/path/to/folder] [/path/to/file.sqlite3] ...
EOF
}

sub command_line_options(){
    my $show_help;

    GetOptions("d=s" => \$destination, "h|help|?" => \$show_help)
        or die("Error in command line arguments\n");

    if($show_help){
        print_help();
        exit(0);
    }

    push(@sources, @ARGV);
    if(!@sources){ @sources = ("/tmp",); }
}

sub resolve_destination_database(){
	if(!$destination){
		# in this version of the script, always assume the database is in the subpath data/data21.sqlite3	
		my $dirname = abs_path(dirname(__FILE__));
		$destination = $dirname . "/data/data21.sqlite3";
	}

    # check the destination database actually exists
    if( ! -f $destination ){
        my $answer = undef;
        do {
            print("WARNING: the destination database `" . $destination . "' does not exist. Do you want to still proceed? (y/n): ");
            $answer = <STDIN>;
            if( $answer =~ /^(y|yes|yeah|yep|1)$/i){ $answer = 1;
            } elsif ($answer =~ /^(n|no|nein|0)$/i){
                print("Aborting...\n");
                exit(0);
            } else { $answer = undef }
        } while (!defined($answer));
    }

}

sub resolve_source_databases(){
    my @cpsources = @sources;
    @sources = ();
    foreach my $path (@cpsources) {
        if( -d $path){
            opendir(my $dh, $path) || confess("Cannot open the directory `$path'");
            for my $entry (readdir($dh)){
                if($entry =~ /\.sqlite3?$/){
                    $entry = $path . "/" . $entry;
                    print("Database found: $entry\n");
                    push(@sources, $entry);
                }
            }
            closedir($dh);
        } elsif ( -f $path ){
            print("Database: $path");
            push(@sources, $path);
        } else {
            print("WARNING: The path `$path' does not exist or it is invalid\n");
        }

    } @sources;
}

# Connect to the given SQLite database
# @param path: the path to the SQLite database
# @return handle to the connection
sub dbi_connect($){
    my ($path) = @_;
    DBI->connect("dbi:SQLite:dbname=$path", "", "", {AutoCommit => 0, RaiseError => 1}) or confess("Connection error to `$path'");
}


# Retrieve all tables defined in the connected database
# @param conn: the SQL connection handle
# @return a hash map with the format `table name' -> `SQL definition'
sub list_tables($){
    my ($conn) = @_;
    map { $_->[2] => $_->[5] } @{$conn->table_info(undef, undef, undef, "TABLE")->fetchall_arrayref()};
}

# Retrieve all views defined in the connected database
# @param conn: the SQL connection handle
# @return a hash map with the format `view name' -> `SQL definition'
sub list_views($){
    my ($conn) = @_;
    map { $_->[2] => $_->[5] } @{$conn->table_info(undef, undef, undef, "VIEW")->fetchall_arrayref()};
}

# Retrieve all columns from the given table
# @param conn: the SQL connection handle
# @param table: the table name (string)
# @return a list ref with all the attribute names
sub list_columns($$){
    my ($conn, $table) = @_;
    map { $_->[3] } @{ $conn->column_info(undef, undef, $table, undef)->fetchall_arrayref() };
}

# Check whether the candidate database is suitable for merging
# @param conn a DBI connection already opened to the candidate database
# @param name the name of the database
# @return true if the database can be merge, false otherwise
sub is_candidate_db($$){
    my ($conn, $db_name) = @_;
    my %tables = list_tables($conn);

    # check that the table executions exist
    if(!$tables{"executions"}){
        print("Skipping the database `$db_name' because it does not have a table `executions'\n");
        return 0;
    }
    # check that the table parameters exist
    if(!$tables{"parameters"}){
        print("Skipping the database `$db_name' because it does not have a table `parameters'\n");
        return 0;
    }

    return 1; # yes
}

# Normalise the table `executions' in the main database copying all parameters from the candidate
# @param cc1 the connection to the candidate database
sub normalize_params($){
    my ($cc1) = @_;
    my %cc1tables = list_tables($cc1);
    my $tbl_temp = "executions_TEMP";

    # Create a table `executions' if it does not already exist
    if(!$conn_tables{"executions"}){
        print("Creating the table `executions' in `$destination' ...\n");
        $conn->do("CREATE TABLE executions (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, time_start TIMESTAMP DEFAULT CURRENT_TIMESTAMP, time_end TIMESTAMP, magic INTEGER NOT NULL DEFAULT 0);");
        $conn->commit();

        $conn_tables{"executions"} = $cc1tables{"executions"};
    }

    # Get all parameters from the candidate database
    my @cc1params = map { @$_ } $cc1->selectall_array("SELECT DISTINCT name FROM parameters ORDER BY 1");
    my @main_columns = list_columns($conn, "executions");
    my @missing_params = sort ( array_minus(@cc1params, @main_columns) );

    if(@missing_params){
        print("Adding the parameters " . join(", ", @missing_params) . " ... \n");

        # Remove all views
        my %views = list_views($conn);
        for my $v (keys %views){
            next if $v eq 'view_executions';
            $conn->do("DROP VIEW $v");
        }
        $conn->do("DROP VIEW IF EXISTS view_executions");

        # make the union of the parameters
        my @parameters = sort (  grep { my $x = $_; ! grep(/^\Q$x\E$/, ("id", "timeStart", "time_start", "timeEnd", "time_end", "magic")) } unique(@cc1params, @main_columns) );

        $conn->do("DROP TABLE $tbl_temp") if($conn_tables{$tbl_temp}); # just in case it already exists

        my $ddl = "CREATE TABLE $tbl_temp (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, time_start TIMESTAMP DEFAULT CURRENT_TIMESTAMP, time_end TIMESTAMP, magic INTEGER NOT NULL DEFAULT 0 ";
        foreach my $param (@parameters){
            $ddl .= ", $param TEXT";
        }
        $ddl .= ")";
        $conn->do($ddl);

        # now copy all existing entries from `executions' into `$tbl_temp'
        my @preamble = map { if ($_ eq "timeStart") { "time_start"} elsif($_ eq "timeEnd") { "time_end" } else { $_ } } @main_columns;
        my $sql = "INSERT INTO $tbl_temp (" . join(", ", @preamble) . ") SELECT " . join(", ", @main_columns) . " FROM executions;";
        $conn->do($sql);
        $conn->do("DROP TABLE executions");
        $conn->do("ALTER TABLE $tbl_temp RENAME TO executions");

        # restore all views
        $conn->do($views{"view_executions"}) if $views{"view_executions"};
        while (my ($view, $def) = each(%views)){
            next if $view eq 'view_executions';
            $conn->do($def);
        }

        $conn->commit();
    }
}
sub mk_tbl_temporary_source($$){
    my ($conn, $records) = @_;
    $conn->do("CREATE TEMPORARY TABLE $tbl_temporary (id INTEGER NOT NULL);");
    $conn->commit();
    my $i = 0; # commit from time to time to speed up insertions
    for my $record (@$records) {
        my $id = shift(@$record);
        $conn->do("INSERT INTO $tbl_temporary(id) VALUES ($id)");
        $i++;
        $conn->commit() if ($i % 1024 == 0);
    }
    $conn->commit() if ($i % 1024 != 0);
}

sub join_tbl_temporary_main($$){
    my ($conn, $exec_columns) = @_;
    # they must be either both equals or both nulls
    my $joincond = join(" AND ", map { "((e.$_ = t.$_) OR (e.$_ IS NULL AND t.$_ IS NULL))" } @$exec_columns );
    my $lst_exec_columns = join(", ", @$exec_columns);
    my $sql = <<EOF;
SELECT id, $lst_exec_columns
FROM (
    SELECT e.id AS exec_id, t.*
    FROM $tbl_temporary t LEFT JOIN executions e ON ( $joincond )
)
WHERE exec_id IS NULL
ORDER BY id
EOF

    $conn->selectall_arrayref($sql);
}

# Code based on Common::merge
# @param conn: the connection to the candidate database
sub merge($){
    my ($cc1) = @_;
    normalize_params($cc1); # adjust the columns in the table executions
    my %cc1tables = list_tables($cc1);

    # The idea is to insert all tuples in a temporary table to check for repetitions
    $conn->do("CREATE TEMPORARY TABLE $tbl_temporary (id INTEGER, time_start TIMESTAMP, time_end TIMESTAMP, magic INTEGER)");
    my $i = 0; # to speed up the insertions, commit once every 1024 added records
    for my $record (@{$cc1->selectall_arrayref("SELECT id, timeStart, timeEnd, magic FROM executions")}){
        $conn->do("INSERT INTO $tbl_temporary (id, time_start, time_end, magic) VALUES (?, ?, ?, ?)", undef, @{$record});
        $i++;
        $conn->commit() if ($i % 1024 == 0);
    }
    $conn->commit() if($i % 1024 != 0);

    # Check the tuples that are not inserted in the main `executions table'
    my @exec_columns = ("time_start", "time_end", "magic");
    my $exec_columns = join(", ", @exec_columns);
    my $records = join_tbl_temporary_main($conn, \@exec_columns);
    if(@$records){
        $i = 0;
        map {printf("%-25s", " " . $_) } @exec_columns; print("\n");
        map { print(  "-"x25 ) } @exec_columns; print("\n");
        for my $record (@$records) {
            my @list = @$record; shift(@list);
            for my $entry (@list){
                printf("%-25s", " " . (!defined($entry) ? "NULL" : $entry) );
            }
            print("\n");
            $i++;
            last if ($i >= 25);
        }
        if(@$records > 25){ print("...\n") ;}
    }
    print("Found " . @$records . " executions not loaded\n");
    if(@$records) {
        mk_tbl_temporary_source($cc1, $records);
        $records = undef; # garbage collect
        my $exec_records = $cc1->selectall_arrayref(<<EOF);
        SELECT timeStart, timeEnd, magic, id FROM executions WHERE id IN (SELECT id FROM $tbl_temporary)
EOF
        my %exec_id_map = (); # map an ID from the source db to an ID into the destination db
        for my $r (@{$exec_records}) {
            my $id_old = pop @{$r};
            # copy the execution
            $conn->do("INSERT INTO executions ($exec_columns) VALUES (?, ?, ?)", undef, @{$r});

            my $id_new = $conn->last_insert_id(undef, undef, undef, undef);
            $exec_id_map{$id_old} = $id_new;

            # copy the parameters
            for my $row ($cc1->selectall_array("SELECT name, value FROM parameters WHERE exec_id = ?", undef, ($id_old))){
                my ($name, $value) = @$row;
                $conn->do("UPDATE executions SET $name = ? WHERE id = ?", undef, ($value, $id_new));
            }

            $i++;
            $conn->commit() if ($i % 1024 == 0);
        }
        $conn->commit() if ($i % 1024 != 0);
        $exec_records = undef; # garbage collect

        # add the other tables
        while (my ($table, $ddl) = each(%cc1tables)) {
            next if ($table =~ /sqlite_/ || $table eq 'executions' || $table eq 'parameters');
            if (!$conn_tables{$table}) {
                print("Creating the table `$table' in `$destination' ...\n");
                $conn->do($ddl);
                $conn->commit();
                $conn_tables{$table} = $ddl;
            }
            print("Copying the records from the table `$table' ...\n");

            my $has_exec_id = 0;
            my @columns = grep {
                my $ignore = $_ eq "exec_id";
                $has_exec_id = 1 if $ignore;
                !$ignore && $_ ne "id";
            } list_columns($cc1, $table);
            push(@columns, "exec_id") if $has_exec_id; # append at the end
            my $columns = join(", ", @columns);
            my $records = $cc1->selectall_arrayref("SELECT " . $columns . " FROM " . $table);
            my $sqlstmt = "INSERT INTO $table ($columns) VALUES (" . join(", ", map {"?"} @columns) . ")";
            for my $r (@{$records}) {
                my @values = @{$r};
                if($has_exec_id){ # replace the loaded execution id with the final execution id in the output database
                    my $exec_id_old = pop(@values);
                    next if !exists($exec_id_map{$exec_id_old}); # the exec id is not in the list of executions to be inserted
                    my $exec_id_new = $exec_id_map{$exec_id_old};
                    push(@values, $exec_id_new);
                }
                $conn->do($sqlstmt, undef, @values);
                $conn->commit() if (++$i % 1024 == 0); # commit once in a while
            }
            $conn->commit() if ($i % 1024 != 0);
        }

    } # end if: there are records to copy

    # Remove the temporary table
    $conn->do("DROP TABLE $tbl_temporary");
}

sub main(){
    command_line_options();
    resolve_destination_database();
    resolve_source_databases();

    # open the connection to the destination database
    $conn = dbi_connect($destination);
    $conn->do("PRAGMA foreign_keys=OFF;");

    %conn_tables = list_tables($conn);

    for(my $index = 0; $index < @sources; $index++){
        my $path_database = $sources[$index];
        print("[$index] Examining the database `$path_database' ...\n");
        my $cc1 = dbi_connect($path_database);
        if(is_candidate_db($cc1, $path_database)){
            merge($cc1);
        }
        $cc1->disconnect(); $cc1 = undef;
    }

    # close the connection to the database
    $conn->disconnect(); $conn = undef;
}

main() if !caller();
