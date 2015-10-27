

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
    $alfavit = @{" "=""; "а"="a"; "б"="b"; "в"="v"; "г"="g"; "д"="d"; 
    "е"="e"; "ё"="yo"; "ж"="zh"; "з"="z"; 
    "и"="i"; "й"="j"; "к"="k"; "л"="l"; 
    "м"="m"; "н"="n"; "о"="o"; "п"="p"; 
    "р"="r"; "с"="s"; "т"="t"; "у"="u"; 
    "ф"="f"; "х"="kh"; "ц"="c"; "ч"="ch"; 
    "ш"="sh"; "щ"="sch"; "ъ"=""; "ь"=""; 
    "ы"="y"; "э"="je"; "ю"="yu"; "я"="ya"}
    
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


# Функция активации учетной записи пользователя.
# Учетная запись ДОЛЖНА СУЩЕСТВОВАТЬ.
# Аргумент '$san' - sAMAccountName из ActiveDirectory
function Usr_activate($dist_name)
{
    $u_object = Get-ADUser -Identity "$dist_name"
    $log_str = "Вызов функции обработки пользователя " + $u_object.Name
    Logging $log_str
    
    if ($u_object.enabled -ne $true)
    {
        $chars = "abcdefghigkABCDEFGHIJK23456789!@$%*"
        $chars = $chars.ToCharArray()
        $pwd_string = ($chars | Get-Random -Count 7) -join ""
        Logging "    Пароль для пользователя $u_object - $pwd_string"
        
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
            $activate_err = "    ERR: ошибка активации 
                или назначения пароля - " + $Error[0].Exception.Message
            Logging $activate_err
        }
    }
    else
    {
        $msg = "    Аккаунт " + $u_object.name + " активен. Обработка следующего."
        Logging $msg
    }
}


# Функция открытия сессии Powershell
# на почтовом сервере (для использования
# командлетов Exchange Management console)
function OpenSession
{
    Logging "Вызов функции открытия сессии"
    $service = "[Exchange server account]"
    
    try
    {
        $pwd = ConvertTo-SecureString -String (Get-Content ".\post")
        $cred = New-Object -TypeName System.Management.Automation.PSCredential -ArgumentList $service, $pwd
        Logging "    Разрешение на открытие сессии создано успешно."
    }
    catch
    {
        $cred_err = "    ERR: ошибка создания разрешения, 
            либо отсутствует файл ключа - " + $Error[0].Exception.Message
        Logging $cred_err
        exit
    }
    
    try
    {
        Get-Credential -Credential $cred

        $session = New-PSSession –ConfigurationName Microsoft.Exchange `
            –ConnectionUri http://exchangehostname.domain.net/PowerShell/ `
            –Credential $cred
    
        Import-PSSession -Session $session `
            -DisableNameChecking `
            -CommandName Enable-Mailbox, Get-Mailbox
        Logging "    Сессия открыта."
    }
    catch
    {
        $session_err = "    ERR: ошибка открытия сессии - " + $Error[0].Exception.Message
        Logging $session_err
        exit
    }
}

# Функция закрытия активной сессии 
# на сервере Exchange
function CloseSession
{
    $active_sessions = Get-PSSession
    if ($active_sessions)
    {
        try
        {
            Remove-PSSession
            Logging "Сессия на сервере Exchange закрыта."
        }
        catch
        {
            $close_session_err = "    ERR: ошибка при попытке закрыть сессию - " + 
                $Error[0].Exception.Message
            Logging $close_session_err
        }
    }
}


# Функция активации (создания) почтового ящика
# существующего пользователя.
# Аргумент '$san' - sAMAccountName из ActiveDirectory
# Аргумент '$alias' - результат, возвращаемый
# функцией 'Translit'
function Mailbox_activate($dist_name, $alias)
{
    $u = Get-ADUser -Identity "$dist_name"
    $log_str = "Вызов функции активации почтового ящика пользователя " + $u.Name
    Logging $log_str
    
    $mb = Get-Mailbox "$dist_name"
    
    if ($mb.name -ne $null) 
    {
        $msg = "    Почтовый ящик " + $mb.name + " уже активирован.
            Переход к следующему."
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
            $mb_activate_err = "    При активации почтового ящика " `
                + $mb.name + " произошла ошибка - " `
                + $Error[0].Exception.Message
            Logging $mb_activate_err
        }
    }
}


# Функция для запросов на выборку данных PG
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

Logging "---Начало выполнения задания---"
Logging "=============================="

try
{
    [diagnostics.process]::start(".\start.bat").WaitForExit()
}
catch
{
    $start_putty_err = "ERR: Ошибка при запуске Putty: " + $Error[0].Exception.Message
    Logging "$start_putty_err"
    exit
}

try
{
    Import-Module ActiveDirectory
}
catch
{
    $ad_module_err = "ERR: Ошибка при импорте модуля AD: " + $Error[0].Exception.Message
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
    Logging "Запрос на выборку пользователей из БД"
    $result = Get-ODBC-Data -query $query
    Logging "Запрос успешно завершен"
}
catch
{
    $db_err = "ERR: Ошибка при выполнении запроса: " + $Error[0].Exception.Message
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
    Logging "Запрос к БД вернул пустой набор!"
    exit
}

CloseSession

Logging "=============================="
Logging "---The END---"





######################-Examples-######################


# Функция DML-запросов PG
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
            ('Проверочная группа'
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
