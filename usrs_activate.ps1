

# Log function
# in file ".\date time.log"
# parameter - String e.g. "somestring".
# NB! string concatenation expression like this "black" + "cat"
# not applicable as parameter
function Logging([string]$log_str)
{
    $today = Get-Date -Format "dd.MM.yyyy"
    $dt = Get-Date -Format "dd.MM.yyyy HH:mm:ss"
    $dt + ' - ' + $log_str | Out-File -FilePath ".\log\report-$today.log" -Append -Encoding unicode
}


# Transliteration function.
# string username "Surname NP"
# turn to email login
function Translit([string]$fio)
{
    $alfavit = @{" "=""; "�"="a"; "�"="b"; "�"="v"; "�"="g"; "�"="d"; 
    "�"="e"; "�"="yo"; "�"="zh"; "�"="z"; 
    "�"="i"; "�"="j"; "�"="k"; "�"="l"; 
    "�"="m"; "�"="n"; "�"="o"; "�"="p"; 
    "�"="r"; "�"="s"; "�"="t"; "�"="u"; 
    "�"="f"; "�"="kh"; "�"="c"; "�"="ch"; 
    "�"="sh"; "�"="sch"; "�"=""; "�"=""; 
    "�"="y"; "�"="je"; "�"="yu"; "�"="ya"}
    
    $alias = ""
    foreach($c in $fio.ToCharArray())
    {
       if ($alfavit.ContainsKey($c.ToString())){
        $alias = $alias + $alfavit[$c.ToString()]
       }
       else{
        $alias = $alias + $c
       }
    }
    
    return $alias
}


# ������� ��������� ������� ������ ������������.
# ������� ������ ������ ������������.
# �������� '$san' - sAMAccountName �� ActiveDirectory
function Usr_activate($dist_name)
{
    $u_object = Get-ADUser -Identity "$dist_name"
    $log_str = "����� ������� ��������� ������������ " + $u_object.Name
    Logging $log_str
    
    if ($u_object.enabled -ne $true)
    {
        $chars = "abcdefghigkABCDEFGHIJK23456789!@$%*"
        $chars = $chars.ToCharArray()
        $pwd_string = ($chars | Get-Random -Count 7) -join ""
        Logging "    ������ ��� ������������ $u_object - $pwd_string"
        
        try
        {
            $uObj = Get-ADUser -Identity "$dist_name"
            Set-ADAccountPassword $uObj.distinguishedName -Reset -NewPassword (ConvertTo-SecureString `
                -AsPlainText $pwd_string -Force)
        
            $uObj = Get-ADUser -Identity "$dist_name"        
            Enable-ADAccount -Identity $uObj.distinguishedName
        }
        catch
        {
            $activate_err = "    ERR: ������ ��������� 
                ��� ���������� ������ - " + $Error[0].Exception.Message
            Logging $activate_err
        }
    }
    else
    {
        $msg = "    ������� " + $u_object.name + " �������. ��������� ����������."
        Logging $msg
    }
}


# ������� �������� ������ Powershell
# �� �������� ������� (��� �������������
# ����������� Exchange Management console)
function OpenSession
{
    Logging "����� ������� �������� ������"
    $service = "[Exchange server account]"
    
    try
    {
        $pwd = ConvertTo-SecureString -String (Get-Content ".\post")
        $cred = New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList $service, $pwd
        Logging "    ���������� �� �������� ������ ������� �������."
    }
    catch
    {
        $cred_err = "    ERR: ������ �������� ����������, 
            ���� ����������� ���� ����� - " + $Error[0].Exception.Message
        Logging $cred_err
        exit
    }
    
    try
    {
        Get-Credential -Credential $cred

        $session = New-PSSession �ConfigurationName Microsoft.Exchange `
            �ConnectionUri http://exchangehostname.domain.net/PowerShell/ `
            �Credential $cred
    
        Import-PSSession -Session $session `
            -DisableNameChecking `
            -CommandName Enable-Mailbox, Get-Mailbox
        Logging "    ������ �������."
    }
    catch
    {
        $session_err = "    ERR: ������ �������� ������ - " + $Error[0].Exception.Message
        Logging $session_err
        exit
    }
}

# ������� �������� �������� ������ 
# �� ������� Exchange
function CloseSession
{
    $active_sessions = Get-PSSession
    if ($active_sessions)
    {
        try
        {
            Remove-PSSession
            Logging "������ �� ������� Exchange �������."
        }
        catch
        {
            $close_session_err = "    ERR: ������ ��� ������� ������� ������ - " + 
                $Error[0].Exception.Message
            Logging $close_session_err
        }
    }
}


# ������� ��������� (��������) ��������� �����
# ������������� ������������.
# �������� '$san' - sAMAccountName �� ActiveDirectory
# �������� '$alias' - ���������, ������������
# �������� 'Translit'
function Mailbox_activate($dist_name, $alias)
{
    $u = Get-ADUser -Identity "$dist_name"
    $log_str = "����� ������� ��������� ��������� ����� ������������ " + $u.Name
    Logging $log_str
    
    $mb = Get-Mailbox "$dist_name"
    
    if ($mb.name -ne $null) 
    {
        $msg = "    �������� ���� " + $mb.name + " ��� �����������.
            ������� � ����������."
        Logging $msg
    } 
    else 
    {
        try
        {
            Enable-Mailbox -Identity "$dist_name" -Confirm:$false -Alias $alias
        }
        catch
        {
            $mb_activate_err = "    ��� ��������� ��������� ����� " `
                + $mb.name + " ��������� ������ - " `
                + $Error[0].Exception.Message
            Logging $mb_activate_err
        }
    }
}


# ������� ��� �������� �� ������� ������ PG
function Get-ODBC-Data{
    param([string]$query)
    $conn = New-Object System.Data.Odbc.OdbcConnection
    $conn.ConnectionString = 'Driver={PostgreSQL 64-Bit ODBC Drivers};
        Server=10.0.0.157;
        Port=5432;
        Database=postgres;
        Uid=dev;
        Pwd=1q2w3e$RT;'
    $conn.open()
    $cmd = New-object System.Data.Odbc.OdbcCommand($query,$conn)
    $ds = New-Object system.Data.DataSet
    (New-Object system.Data.odbc.odbcDataAdapter($cmd)).fill($ds) | out-null
    $conn.close()
    $ds.Tables[0]
}




######################-Begin-######################

New-Item -ItemType directory log -Force | out-null

Logging "---������ ���������� �������---"
Logging "=============================="

try
{
    [diagnostics.process]::start(".\start.bat").WaitForExit()
}
catch
{
    $start_putty_err = "ERR: ������ ��� ������� Putty: " + $Error[0].Exception.Message
    Logging "$start_putty_err"
    exit
}

try
{
    Import-Module ActiveDirectory
}
catch
{
    $ad_module_err = "ERR: ������ ��� ������� ������ AD: " + $Error[0].Exception.Message
    Logging "$ad_module_err"
    exit
}

OpenSession

$query = "SELECT au.distinguished_name
            FROM personal.person_san ps
                , personal.ad_users au
            WHERE ps.person_id IN (
                SELECT em.person_id FROM personal.employees em
                WHERE (em.leave_date IS NULL OR current_date BETWEEN em.recept_date AND em.leave_date)
                GROUP BY em.person_id )
            AND ps.san = au.san"
try
{
    Logging "������ �� ������� ������������� �� ��"
    $result = Get-ODBC-Data -query $query
    Logging "������ ������� ��������"
}
catch
{
    $db_err = "ERR: ������ ��� ���������� �������: " + $Error[0].Exception.Message
    Logging "$db_err"
    exit
}


if ($result.count -ne 0)
{
    foreach ($dn in $result)
    {
        $curr_dn = $dn.distinguished_name
        $current_user = Get-ADUser -Identity "$curr_dn"
        $curr_san = $current_user.SamAccountName
        $mail_alias = Translit $curr_san

        Usr_activate $curr_dn
        Mailbox_activate $curr_dn $mail_alias
    }
}
else
{
    Logging "������ � �� ������ ������ �����!"
    exit
}

CloseSession

Logging "=============================="
Logging "---The END---"





######################-Examples-######################


# ������� DML-�������� PG
<#function Set-ODBC-Data{
    param([string]$query=$(throw 'query is required.'))
    $conn = New-Object System.Data.Odbc.OdbcConnection
    $conn.ConnectionString= 'Driver={PostgreSQL 64-Bit ODBC Drivers};
        Server=10.0.0.157;
        Port=5432;
        Database=postgres;
        Uid=dev;
        Pwd=1q2w3e$RT;'
    $conn.open()
    $cmd = New-Object System.Data.Odbc.OdbcCommand($query,$conn)    
    $cmd.ExecuteNonQuery()
    $conn.close()
}#>

#DML query example
<#$dml_query = "INSERT INTO personal.ad_groups 
            (gname
            ,san
            ,dn
            cn) 
            VALUES 
            ('����������� ������'
            ,'1'
            ,'2'
            ,'3')"
set-odbc-data -query $query#>

<#$u_object = Get-ADUser -Identity "CN=Usert1,OU=Test Users,DC=kursksmu,DC=net"

    if ($u_object.enabled -ne $true)
    {
        Write-Host "disabled"
    }
    else
    {
        Write-Host "enabled!"
    }#>
