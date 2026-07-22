
namespace InfluxdbDataSync
{
    partial class FormMain
    {
        /// <summary>
        ///  Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        ///  Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        ///  Required method for Designer support - do not modify
        ///  the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(FormMain));
            groupBox1 = new GroupBox();
            btnSelectBackupFile = new Button();
            txtBackupFilePath = new TextBox();
            label15 = new Label();
            label17 = new Label();
            txtRemoteMeasure = new TextBox();
            txtRemotePassword = new TextBox();
            label6 = new Label();
            txtRemoteUsername = new TextBox();
            label5 = new Label();
            txtRemoteDatabase = new TextBox();
            label4 = new Label();
            txtRemotePort = new TextBox();
            label3 = new Label();
            txtRemoteHost = new TextBox();
            label2 = new Label();
            label1 = new Label();
            groupBox2 = new GroupBox();
            btnSelectRestoreFile = new Button();
            txtRestoreFilePath = new TextBox();
            label18 = new Label();
            label16 = new Label();
            txtLocalMeasure = new TextBox();
            txtLocalPassword = new TextBox();
            label11 = new Label();
            txtLocalUsername = new TextBox();
            label10 = new Label();
            txtLocalDatabase = new TextBox();
            label9 = new Label();
            txtLocalPort = new TextBox();
            label8 = new Label();
            txtLocalHost = new TextBox();
            label7 = new Label();
            groupBox3 = new GroupBox();
            dtpEndTime = new DateTimePicker();
            label13 = new Label();
            dtpStartTime = new DateTimePicker();
            label12 = new Label();
            rtbMeasurements = new RichTextBox();
            label14 = new Label();
            btnSync = new Button();
            btnCancelSync = new Button();
            groupBox4 = new GroupBox();
            rtbLog = new RichTextBox();
            btnBackupToFile = new Button();
            btnRestoreFromFile = new Button();
            btnFillData = new Button();
            groupBox5 = new GroupBox();
            numWriteBatchSize = new NumericUpDown();
            labelWriteBatchSize = new Label();
            numMigrationConcurrency = new NumericUpDown();
            labelMigrationConcurrency = new Label();
            numSyncWindowHours = new NumericUpDown();
            labelSyncWindowHours = new Label();
            txtTimeInterval = new TextBox();
            label23 = new Label();
            numMax = new NumericUpDown();
            numMin = new NumericUpDown();
            label19 = new Label();
            label20 = new Label();
            groupBox1.SuspendLayout();
            groupBox2.SuspendLayout();
            groupBox3.SuspendLayout();
            groupBox4.SuspendLayout();
            groupBox5.SuspendLayout();
            ((System.ComponentModel.ISupportInitialize)numWriteBatchSize).BeginInit();
            ((System.ComponentModel.ISupportInitialize)numMigrationConcurrency).BeginInit();
            ((System.ComponentModel.ISupportInitialize)numSyncWindowHours).BeginInit();
            ((System.ComponentModel.ISupportInitialize)numMax).BeginInit();
            ((System.ComponentModel.ISupportInitialize)numMin).BeginInit();
            SuspendLayout();
            // 
            // groupBox1
            // 
            groupBox1.Controls.Add(btnSelectBackupFile);
            groupBox1.Controls.Add(txtBackupFilePath);
            groupBox1.Controls.Add(label15);
            groupBox1.Controls.Add(label17);
            groupBox1.Controls.Add(txtRemoteMeasure);
            groupBox1.Controls.Add(txtRemotePassword);
            groupBox1.Controls.Add(label6);
            groupBox1.Controls.Add(txtRemoteUsername);
            groupBox1.Controls.Add(label5);
            groupBox1.Controls.Add(txtRemoteDatabase);
            groupBox1.Controls.Add(label4);
            groupBox1.Controls.Add(txtRemotePort);
            groupBox1.Controls.Add(label3);
            groupBox1.Controls.Add(txtRemoteHost);
            groupBox1.Controls.Add(label2);
            groupBox1.Controls.Add(label1);
            groupBox1.Location = new Point(16, 13);
            groupBox1.Margin = new Padding(5, 6, 5, 6);
            groupBox1.Name = "groupBox1";
            groupBox1.Padding = new Padding(5, 6, 5, 6);
            groupBox1.Size = new Size(471, 170);
            groupBox1.TabIndex = 0;
            groupBox1.TabStop = false;
            groupBox1.Text = "远程InfluxDB配置";
            // 
            // btnSelectBackupFile
            // 
            btnSelectBackupFile.Location = new Point(385, 130);
            btnSelectBackupFile.Margin = new Padding(5, 6, 5, 6);
            btnSelectBackupFile.Name = "btnSelectBackupFile";
            btnSelectBackupFile.Size = new Size(68, 23);
            btnSelectBackupFile.TabIndex = 5;
            btnSelectBackupFile.Text = "选择";
            btnSelectBackupFile.UseVisualStyleBackColor = true;
            btnSelectBackupFile.Click += btnSelectBackupFile_Click;
            // 
            // txtBackupFilePath
            // 
            txtBackupFilePath.Location = new Point(84, 129);
            txtBackupFilePath.Margin = new Padding(5, 6, 5, 6);
            txtBackupFilePath.Name = "txtBackupFilePath";
            txtBackupFilePath.ReadOnly = true;
            txtBackupFilePath.Size = new Size(291, 23);
            txtBackupFilePath.TabIndex = 15;
            // 
            // label15
            // 
            label15.Location = new Point(18, 129);
            label15.Margin = new Padding(5, 0, 5, 0);
            label15.Name = "label15";
            label15.Size = new Size(68, 24);
            label15.TabIndex = 14;
            label15.Text = "保存文件：";
            label15.TextAlign = ContentAlignment.MiddleLeft;
            // 
            // label17
            // 
            label17.Location = new Point(268, 57);
            label17.Margin = new Padding(5, 0, 5, 0);
            label17.Name = "label17";
            label17.Size = new Size(44, 17);
            label17.TabIndex = 13;
            label17.Text = "序列";
            label17.TextAlign = ContentAlignment.MiddleLeft;
            // 
            // txtRemoteMeasure
            // 
            txtRemoteMeasure.Location = new Point(322, 58);
            txtRemoteMeasure.Margin = new Padding(5, 6, 5, 6);
            txtRemoteMeasure.Name = "txtRemoteMeasure";
            txtRemoteMeasure.Size = new Size(124, 23);
            txtRemoteMeasure.TabIndex = 12;
            // 
            // txtRemotePassword
            // 
            txtRemotePassword.Location = new Point(322, 91);
            txtRemotePassword.Margin = new Padding(5, 6, 5, 6);
            txtRemotePassword.Name = "txtRemotePassword";
            txtRemotePassword.Size = new Size(124, 23);
            txtRemotePassword.TabIndex = 10;
            txtRemotePassword.UseSystemPasswordChar = true;
            // 
            // label6
            // 
            label6.Location = new Point(268, 90);
            label6.Margin = new Padding(5, 0, 5, 0);
            label6.Name = "label6";
            label6.Size = new Size(51, 24);
            label6.TabIndex = 9;
            label6.Text = "密码：";
            label6.TextAlign = ContentAlignment.MiddleLeft;
            // 
            // txtRemoteUsername
            // 
            txtRemoteUsername.Location = new Point(84, 91);
            txtRemoteUsername.Margin = new Padding(5, 6, 5, 6);
            txtRemoteUsername.Name = "txtRemoteUsername";
            txtRemoteUsername.Size = new Size(162, 23);
            txtRemoteUsername.TabIndex = 8;
            // 
            // label5
            // 
            label5.Location = new Point(18, 90);
            label5.Margin = new Padding(5, 0, 5, 0);
            label5.Name = "label5";
            label5.Size = new Size(65, 24);
            label5.TabIndex = 7;
            label5.Text = "用户名：";
            label5.TextAlign = ContentAlignment.MiddleLeft;
            // 
            // txtRemoteDatabase
            // 
            txtRemoteDatabase.Location = new Point(84, 58);
            txtRemoteDatabase.Margin = new Padding(5, 6, 5, 6);
            txtRemoteDatabase.Name = "txtRemoteDatabase";
            txtRemoteDatabase.Size = new Size(162, 23);
            txtRemoteDatabase.TabIndex = 6;
            // 
            // label4
            // 
            label4.Location = new Point(18, 57);
            label4.Margin = new Padding(5, 0, 5, 0);
            label4.Name = "label4";
            label4.Size = new Size(68, 24);
            label4.TabIndex = 5;
            label4.Text = "数据库名：";
            label4.TextAlign = ContentAlignment.MiddleLeft;
            // 
            // txtRemotePort
            // 
            txtRemotePort.Location = new Point(322, 25);
            txtRemotePort.Margin = new Padding(5, 6, 5, 6);
            txtRemotePort.Name = "txtRemotePort";
            txtRemotePort.Size = new Size(124, 23);
            txtRemotePort.TabIndex = 4;
            // 
            // label3
            // 
            label3.Location = new Point(268, 23);
            label3.Margin = new Padding(5, 0, 5, 0);
            label3.Name = "label3";
            label3.Size = new Size(51, 24);
            label3.TabIndex = 3;
            label3.Text = "端口：";
            label3.TextAlign = ContentAlignment.MiddleLeft;
            // 
            // txtRemoteHost
            // 
            txtRemoteHost.Location = new Point(84, 25);
            txtRemoteHost.Margin = new Padding(5, 6, 5, 6);
            txtRemoteHost.Name = "txtRemoteHost";
            txtRemoteHost.Size = new Size(162, 23);
            txtRemoteHost.TabIndex = 2;
            // 
            // label2
            // 
            label2.Location = new Point(18, 24);
            label2.Margin = new Padding(5, 0, 5, 0);
            label2.Name = "label2";
            label2.Size = new Size(65, 24);
            label2.TabIndex = 1;
            label2.Text = "主机名：";
            label2.TextAlign = ContentAlignment.MiddleLeft;
            // 
            // label1
            // 
            label1.AutoSize = true;
            label1.Location = new Point(8, 33);
            label1.Margin = new Padding(5, 0, 5, 0);
            label1.Name = "label1";
            label1.Size = new Size(0, 17);
            label1.TabIndex = 0;
            // 
            // groupBox2
            // 
            groupBox2.Controls.Add(btnSelectRestoreFile);
            groupBox2.Controls.Add(txtRestoreFilePath);
            groupBox2.Controls.Add(label18);
            groupBox2.Controls.Add(label16);
            groupBox2.Controls.Add(txtLocalMeasure);
            groupBox2.Controls.Add(txtLocalPassword);
            groupBox2.Controls.Add(label11);
            groupBox2.Controls.Add(txtLocalUsername);
            groupBox2.Controls.Add(label10);
            groupBox2.Controls.Add(txtLocalDatabase);
            groupBox2.Controls.Add(label9);
            groupBox2.Controls.Add(txtLocalPort);
            groupBox2.Controls.Add(label8);
            groupBox2.Controls.Add(txtLocalHost);
            groupBox2.Controls.Add(label7);
            groupBox2.Location = new Point(523, 13);
            groupBox2.Margin = new Padding(5, 6, 5, 6);
            groupBox2.Name = "groupBox2";
            groupBox2.Padding = new Padding(5, 6, 5, 6);
            groupBox2.Size = new Size(471, 170);
            groupBox2.TabIndex = 1;
            groupBox2.TabStop = false;
            groupBox2.Text = "本地InfluxDB配置";
            // 
            // btnSelectRestoreFile
            // 
            btnSelectRestoreFile.Location = new Point(387, 134);
            btnSelectRestoreFile.Margin = new Padding(5, 6, 5, 6);
            btnSelectRestoreFile.Name = "btnSelectRestoreFile";
            btnSelectRestoreFile.Size = new Size(68, 23);
            btnSelectRestoreFile.TabIndex = 17;
            btnSelectRestoreFile.Text = "选择";
            btnSelectRestoreFile.UseVisualStyleBackColor = true;
            btnSelectRestoreFile.Click += btnSelectRestoreFile_Click;
            // 
            // txtRestoreFilePath
            // 
            txtRestoreFilePath.Location = new Point(88, 134);
            txtRestoreFilePath.Margin = new Padding(5, 6, 5, 6);
            txtRestoreFilePath.Name = "txtRestoreFilePath";
            txtRestoreFilePath.ReadOnly = true;
            txtRestoreFilePath.Size = new Size(291, 23);
            txtRestoreFilePath.TabIndex = 16;
            // 
            // label18
            // 
            label18.Location = new Point(277, 61);
            label18.Margin = new Padding(5, 0, 5, 0);
            label18.Name = "label18";
            label18.Size = new Size(44, 17);
            label18.TabIndex = 15;
            label18.Text = "序列";
            label18.TextAlign = ContentAlignment.MiddleLeft;
            // 
            // label16
            // 
            label16.AutoSize = true;
            label16.Location = new Point(10, 137);
            label16.Margin = new Padding(5, 0, 5, 0);
            label16.Name = "label16";
            label16.Size = new Size(68, 17);
            label16.TabIndex = 1;
            label16.Text = "加载文件：";
            // 
            // txtLocalMeasure
            // 
            txtLocalMeasure.Location = new Point(331, 63);
            txtLocalMeasure.Margin = new Padding(5, 6, 5, 6);
            txtLocalMeasure.Name = "txtLocalMeasure";
            txtLocalMeasure.Size = new Size(124, 23);
            txtLocalMeasure.TabIndex = 14;
            // 
            // txtLocalPassword
            // 
            txtLocalPassword.Location = new Point(331, 96);
            txtLocalPassword.Margin = new Padding(5, 6, 5, 6);
            txtLocalPassword.Name = "txtLocalPassword";
            txtLocalPassword.Size = new Size(124, 23);
            txtLocalPassword.TabIndex = 10;
            txtLocalPassword.UseSystemPasswordChar = true;
            // 
            // label11
            // 
            label11.Location = new Point(277, 94);
            label11.Margin = new Padding(5, 0, 5, 0);
            label11.Name = "label11";
            label11.Size = new Size(44, 17);
            label11.TabIndex = 9;
            label11.Text = "密码：";
            // 
            // txtLocalUsername
            // 
            txtLocalUsername.Location = new Point(88, 95);
            txtLocalUsername.Margin = new Padding(5, 6, 5, 6);
            txtLocalUsername.Name = "txtLocalUsername";
            txtLocalUsername.Size = new Size(162, 23);
            txtLocalUsername.TabIndex = 8;
            // 
            // label10
            // 
            label10.Location = new Point(10, 94);
            label10.Margin = new Padding(5, 0, 5, 0);
            label10.Name = "label10";
            label10.Size = new Size(56, 17);
            label10.TabIndex = 7;
            label10.Text = "用户名：";
            // 
            // txtLocalDatabase
            // 
            txtLocalDatabase.Location = new Point(88, 62);
            txtLocalDatabase.Margin = new Padding(5, 6, 5, 6);
            txtLocalDatabase.Name = "txtLocalDatabase";
            txtLocalDatabase.Size = new Size(162, 23);
            txtLocalDatabase.TabIndex = 6;
            // 
            // label9
            // 
            label9.Location = new Point(10, 61);
            label9.Margin = new Padding(5, 0, 5, 0);
            label9.Name = "label9";
            label9.Size = new Size(68, 17);
            label9.TabIndex = 5;
            label9.Text = "数据库名：";
            // 
            // txtLocalPort
            // 
            txtLocalPort.Location = new Point(331, 25);
            txtLocalPort.Margin = new Padding(5, 6, 5, 6);
            txtLocalPort.Name = "txtLocalPort";
            txtLocalPort.Size = new Size(124, 23);
            txtLocalPort.TabIndex = 4;
            // 
            // label8
            // 
            label8.Location = new Point(277, 28);
            label8.Margin = new Padding(5, 0, 5, 0);
            label8.Name = "label8";
            label8.Size = new Size(44, 17);
            label8.TabIndex = 3;
            label8.Text = "端口：";
            // 
            // txtLocalHost
            // 
            txtLocalHost.Location = new Point(88, 25);
            txtLocalHost.Margin = new Padding(5, 6, 5, 6);
            txtLocalHost.Name = "txtLocalHost";
            txtLocalHost.Size = new Size(162, 23);
            txtLocalHost.TabIndex = 2;
            // 
            // label7
            // 
            label7.Location = new Point(10, 28);
            label7.Margin = new Padding(5, 0, 5, 0);
            label7.Name = "label7";
            label7.Size = new Size(56, 17);
            label7.TabIndex = 1;
            label7.Text = "主机名：";
            // 
            // groupBox3
            // 
            groupBox3.Controls.Add(dtpEndTime);
            groupBox3.Controls.Add(label13);
            groupBox3.Controls.Add(dtpStartTime);
            groupBox3.Controls.Add(label12);
            groupBox3.Controls.Add(rtbMeasurements);
            groupBox3.Controls.Add(label14);
            groupBox3.Location = new Point(16, 269);
            groupBox3.Margin = new Padding(5, 6, 5, 6);
            groupBox3.Name = "groupBox3";
            groupBox3.Padding = new Padding(5, 6, 5, 6);
            groupBox3.Size = new Size(978, 163);
            groupBox3.TabIndex = 2;
            groupBox3.TabStop = false;
            groupBox3.Text = "数据查询配置";
            // 
            // dtpEndTime
            // 
            dtpEndTime.CustomFormat = "yyyy-MM-dd HH:mm:ss";
            dtpEndTime.Format = DateTimePickerFormat.Custom;
            dtpEndTime.Location = new Point(435, 22);
            dtpEndTime.Margin = new Padding(5, 6, 5, 6);
            dtpEndTime.Name = "dtpEndTime";
            dtpEndTime.Size = new Size(188, 23);
            dtpEndTime.TabIndex = 5;
            // 
            // label13
            // 
            label13.Location = new Point(347, 22);
            label13.Margin = new Padding(5, 0, 5, 0);
            label13.Name = "label13";
            label13.Size = new Size(68, 17);
            label13.TabIndex = 4;
            label13.Text = "结束时间：";
            // 
            // dtpStartTime
            // 
            dtpStartTime.CustomFormat = "yyyy-MM-dd HH:mm:ss";
            dtpStartTime.Format = DateTimePickerFormat.Custom;
            dtpStartTime.Location = new Point(112, 22);
            dtpStartTime.Margin = new Padding(5, 6, 5, 6);
            dtpStartTime.Name = "dtpStartTime";
            dtpStartTime.Size = new Size(172, 23);
            dtpStartTime.TabIndex = 3;
            // 
            // label12
            // 
            label12.Location = new Point(18, 22);
            label12.Margin = new Padding(5, 0, 5, 0);
            label12.Name = "label12";
            label12.Size = new Size(68, 17);
            label12.TabIndex = 2;
            label12.Text = "开始时间：";
            // 
            // rtbMeasurements
            // 
            rtbMeasurements.Location = new Point(24, 74);
            rtbMeasurements.Margin = new Padding(5, 6, 5, 6);
            rtbMeasurements.Name = "rtbMeasurements";
            rtbMeasurements.Size = new Size(949, 77);
            rtbMeasurements.TabIndex = 1;
            rtbMeasurements.Text = "";
            // 
            // label14
            // 
            label14.Location = new Point(18, 51);
            label14.Margin = new Padding(5, 0, 5, 0);
            label14.Name = "label14";
            label14.Size = new Size(248, 17);
            label14.TabIndex = 0;
            label14.Text = "Tag 筛选（每行一个，留空同步全部）：";
            // 
            // btnSync
            // 
            btnSync.Location = new Point(324, 444);
            btnSync.Margin = new Padding(5, 6, 5, 6);
            btnSync.Name = "btnSync";
            btnSync.Size = new Size(163, 28);
            btnSync.TabIndex = 3;
            btnSync.Text = "开始同步数据";
            btnSync.UseVisualStyleBackColor = true;
            btnSync.Click += btnSync_Click;
            //
            // btnCancelSync
            //
            btnCancelSync.Enabled = false;
            btnCancelSync.Location = new Point(837, 444);
            btnCancelSync.Margin = new Padding(5, 6, 5, 6);
            btnCancelSync.Name = "btnCancelSync";
            btnCancelSync.Size = new Size(145, 28);
            btnCancelSync.TabIndex = 9;
            btnCancelSync.Text = "取消同步";
            btnCancelSync.UseVisualStyleBackColor = true;
            btnCancelSync.Click += btnCancelSync_Click;
            // 
            // groupBox4
            // 
            groupBox4.Controls.Add(rtbLog);
            groupBox4.Location = new Point(16, 469);
            groupBox4.Margin = new Padding(5, 6, 5, 6);
            groupBox4.Name = "groupBox4";
            groupBox4.Padding = new Padding(5, 6, 5, 6);
            groupBox4.Size = new Size(978, 262);
            groupBox4.TabIndex = 4;
            groupBox4.TabStop = false;
            groupBox4.Text = "操作日志";
            // 
            // rtbLog
            // 
            rtbLog.Location = new Point(24, 28);
            rtbLog.Margin = new Padding(5, 6, 5, 6);
            rtbLog.Name = "rtbLog";
            rtbLog.Size = new Size(949, 214);
            rtbLog.TabIndex = 0;
            rtbLog.Text = "";
            // 
            // btnBackupToFile
            // 
            btnBackupToFile.Location = new Point(151, 444);
            btnBackupToFile.Margin = new Padding(5, 6, 5, 6);
            btnBackupToFile.Name = "btnBackupToFile";
            btnBackupToFile.Size = new Size(163, 28);
            btnBackupToFile.TabIndex = 5;
            btnBackupToFile.Text = "开始备份数据";
            btnBackupToFile.UseVisualStyleBackColor = true;
            btnBackupToFile.Click += btnBackupToFile_Click;
            // 
            // btnRestoreFromFile
            // 
            btnRestoreFromFile.Location = new Point(497, 444);
            btnRestoreFromFile.Margin = new Padding(5, 6, 5, 6);
            btnRestoreFromFile.Name = "btnRestoreFromFile";
            btnRestoreFromFile.Size = new Size(163, 28);
            btnRestoreFromFile.TabIndex = 6;
            btnRestoreFromFile.Text = "开始还原数据";
            btnRestoreFromFile.UseVisualStyleBackColor = true;
            btnRestoreFromFile.Click += btnRestoreFromFile_Click;
            // 
            // btnFillData
            // 
            btnFillData.Location = new Point(670, 444);
            btnFillData.Margin = new Padding(5, 6, 5, 6);
            btnFillData.Name = "btnFillData";
            btnFillData.Size = new Size(163, 28);
            btnFillData.TabIndex = 7;
            btnFillData.Text = "生成随机数据";
            btnFillData.UseVisualStyleBackColor = true;
            btnFillData.Click += btnFillData_Click;
            // 
            // groupBox5
            // 
            groupBox5.Controls.Add(numWriteBatchSize);
            groupBox5.Controls.Add(labelWriteBatchSize);
            groupBox5.Controls.Add(numMigrationConcurrency);
            groupBox5.Controls.Add(labelMigrationConcurrency);
            groupBox5.Controls.Add(numSyncWindowHours);
            groupBox5.Controls.Add(labelSyncWindowHours);
            groupBox5.Controls.Add(txtTimeInterval);
            groupBox5.Controls.Add(label23);
            groupBox5.Controls.Add(numMax);
            groupBox5.Controls.Add(numMin);
            groupBox5.Controls.Add(label19);
            groupBox5.Controls.Add(label20);
            groupBox5.Location = new Point(16, 192);
            groupBox5.Name = "groupBox5";
            groupBox5.Size = new Size(978, 78);
            groupBox5.TabIndex = 8;
            groupBox5.TabStop = false;
            groupBox5.Text = "同步配置";
            //
            // numWriteBatchSize
            //
            numWriteBatchSize.Increment = new decimal(new int[] { 1000, 0, 0, 0 });
            numWriteBatchSize.Location = new Point(289, 48);
            numWriteBatchSize.Maximum = new decimal(new int[] { 50000, 0, 0, 0 });
            numWriteBatchSize.Minimum = new decimal(new int[] { 1000, 0, 0, 0 });
            numWriteBatchSize.Name = "numWriteBatchSize";
            numWriteBatchSize.Size = new Size(86, 23);
            numWriteBatchSize.TabIndex = 18;
            numWriteBatchSize.Value = new decimal(new int[] { 10000, 0, 0, 0 });
            //
            // labelWriteBatchSize
            //
            labelWriteBatchSize.Location = new Point(216, 51);
            labelWriteBatchSize.Margin = new Padding(5, 0, 5, 0);
            labelWriteBatchSize.Name = "labelWriteBatchSize";
            labelWriteBatchSize.Size = new Size(74, 17);
            labelWriteBatchSize.TabIndex = 17;
            labelWriteBatchSize.Text = "写入批次：";
            //
            // numMigrationConcurrency
            //
            numMigrationConcurrency.Location = new Point(84, 48);
            numMigrationConcurrency.Maximum = new decimal(new int[] { 16, 0, 0, 0 });
            numMigrationConcurrency.Minimum = new decimal(new int[] { 1, 0, 0, 0 });
            numMigrationConcurrency.Name = "numMigrationConcurrency";
            numMigrationConcurrency.Size = new Size(120, 23);
            numMigrationConcurrency.TabIndex = 16;
            numMigrationConcurrency.Value = new decimal(new int[] { 4, 0, 0, 0 });
            //
            // labelMigrationConcurrency
            //
            labelMigrationConcurrency.Location = new Point(18, 51);
            labelMigrationConcurrency.Margin = new Padding(5, 0, 5, 0);
            labelMigrationConcurrency.Name = "labelMigrationConcurrency";
            labelMigrationConcurrency.Size = new Size(58, 17);
            labelMigrationConcurrency.TabIndex = 15;
            labelMigrationConcurrency.Text = "并发数：";
            //
            // numSyncWindowHours
            //
            numSyncWindowHours.Location = new Point(910, 18);
            numSyncWindowHours.Maximum = new decimal(new int[] { 720, 0, 0, 0 });
            numSyncWindowHours.Minimum = new decimal(new int[] { 1, 0, 0, 0 });
            numSyncWindowHours.Name = "numSyncWindowHours";
            numSyncWindowHours.Size = new Size(55, 23);
            numSyncWindowHours.TabIndex = 14;
            numSyncWindowHours.Value = new decimal(new int[] { 6, 0, 0, 0 });
            //
            // labelSyncWindowHours
            //
            labelSyncWindowHours.AutoSize = true;
            labelSyncWindowHours.Location = new Point(785, 20);
            labelSyncWindowHours.Name = "labelSyncWindowHours";
            labelSyncWindowHours.Size = new Size(116, 17);
            labelSyncWindowHours.TabIndex = 13;
            labelSyncWindowHours.Text = "单次查询周期（小时）：";
            // 
            // txtTimeInterval
            // 
            txtTimeInterval.Location = new Point(613, 17);
            txtTimeInterval.Margin = new Padding(5, 6, 5, 6);
            txtTimeInterval.Name = "txtTimeInterval";
            txtTimeInterval.Size = new Size(162, 23);
            txtTimeInterval.TabIndex = 12;
            // 
            // label23
            // 
            label23.Location = new Point(435, 20);
            label23.Margin = new Padding(5, 0, 5, 0);
            label23.Name = "label23";
            label23.Size = new Size(168, 17);
            label23.TabIndex = 11;
            label23.Text = "采样时间间隔（秒，0=原始数据）：";
            // 
            // numMax
            // 
            numMax.Increment = new decimal(new int[] { 1, 0, 0, 65536 });
            numMax.Location = new Point(255, 19);
            numMax.Name = "numMax";
            numMax.Size = new Size(120, 23);
            numMax.TabIndex = 10;
            // 
            // numMin
            // 
            numMin.Increment = new decimal(new int[] { 1, 0, 0, 65536 });
            numMin.Location = new Point(84, 18);
            numMin.Name = "numMin";
            numMin.Size = new Size(120, 23);
            numMin.TabIndex = 9;
            // 
            // label19
            // 
            label19.Location = new Point(216, 19);
            label19.Margin = new Padding(5, 0, 5, 0);
            label19.Name = "label19";
            label19.Size = new Size(30, 24);
            label19.TabIndex = 7;
            label19.Text = "——";
            label19.TextAlign = ContentAlignment.MiddleLeft;
            // 
            // label20
            // 
            label20.Location = new Point(18, 17);
            label20.Margin = new Padding(5, 0, 5, 0);
            label20.Name = "label20";
            label20.Size = new Size(76, 24);
            label20.TabIndex = 5;
            label20.Text = "数据范围：";
            label20.TextAlign = ContentAlignment.MiddleLeft;
            // 
            // FormMain
            // 
            AutoScaleDimensions = new SizeF(7F, 17F);
            AutoScaleMode = AutoScaleMode.Font;
            ClientSize = new Size(1004, 740);
            Controls.Add(groupBox5);
            Controls.Add(btnCancelSync);
            Controls.Add(btnFillData);
            Controls.Add(btnRestoreFromFile);
            Controls.Add(btnBackupToFile);
            Controls.Add(groupBox4);
            Controls.Add(btnSync);
            Controls.Add(groupBox3);
            Controls.Add(groupBox2);
            Controls.Add(groupBox1);
            FormBorderStyle = FormBorderStyle.FixedSingle;
            Icon = (Icon)resources.GetObject("$this.Icon");
            Margin = new Padding(5, 6, 5, 6);
            MaximizeBox = false;
            Name = "FormMain";
            Text = "InfluxDB数据同步与备份恢复工具";
            groupBox1.ResumeLayout(false);
            groupBox1.PerformLayout();
            groupBox2.ResumeLayout(false);
            groupBox2.PerformLayout();
            groupBox3.ResumeLayout(false);
            groupBox4.ResumeLayout(false);
            groupBox5.ResumeLayout(false);
            groupBox5.PerformLayout();
            ((System.ComponentModel.ISupportInitialize)numWriteBatchSize).EndInit();
            ((System.ComponentModel.ISupportInitialize)numMigrationConcurrency).EndInit();
            ((System.ComponentModel.ISupportInitialize)numSyncWindowHours).EndInit();
            ((System.ComponentModel.ISupportInitialize)numMax).EndInit();
            ((System.ComponentModel.ISupportInitialize)numMin).EndInit();
            ResumeLayout(false);
        }


        private System.Windows.Forms.GroupBox groupBox1;
        private System.Windows.Forms.TextBox txtRemotePassword;
        private System.Windows.Forms.Label label6;
        private System.Windows.Forms.TextBox txtRemoteUsername;
        private System.Windows.Forms.Label label5;
        private System.Windows.Forms.TextBox txtRemoteDatabase;
        private System.Windows.Forms.Label label4;
        private System.Windows.Forms.TextBox txtRemotePort;
        private System.Windows.Forms.Label label3;
        private System.Windows.Forms.TextBox txtRemoteHost;
        private System.Windows.Forms.Label label2;
        private System.Windows.Forms.Label label1;
        private System.Windows.Forms.GroupBox groupBox2;
        private System.Windows.Forms.TextBox txtLocalPassword;
        private System.Windows.Forms.Label label11;
        private System.Windows.Forms.TextBox txtLocalUsername;
        private System.Windows.Forms.Label label10;
        private System.Windows.Forms.TextBox txtLocalDatabase;
        private System.Windows.Forms.Label label9;
        private System.Windows.Forms.TextBox txtLocalPort;
        private System.Windows.Forms.Label label8;
        private System.Windows.Forms.TextBox txtLocalHost;
        private System.Windows.Forms.Label label7;
        private System.Windows.Forms.GroupBox groupBox3;
        private System.Windows.Forms.DateTimePicker dtpEndTime;
        private System.Windows.Forms.Label label13;
        private System.Windows.Forms.DateTimePicker dtpStartTime;
        private System.Windows.Forms.Label label12;
        private System.Windows.Forms.RichTextBox rtbMeasurements;
        private System.Windows.Forms.Label label14;
        private System.Windows.Forms.Button btnSync;
        private System.Windows.Forms.GroupBox groupBox4;
        private System.Windows.Forms.RichTextBox rtbLog;
        private System.Windows.Forms.GroupBox groupBox5;
        private System.Windows.Forms.TextBox txtRestoreFilePath;
        private System.Windows.Forms.Label label22;
        private System.Windows.Forms.TextBox txtRestorePassword;
        private System.Windows.Forms.Label label21;
        private System.Windows.Forms.TextBox txtRestoreUsername;
        private System.Windows.Forms.Label label20;
        private System.Windows.Forms.TextBox txtRestoreDatabase;
        private System.Windows.Forms.Label label19;
        private System.Windows.Forms.TextBox txtRestorePort;
        private System.Windows.Forms.Label label18;
        private System.Windows.Forms.TextBox txtRestoreHost;
        private System.Windows.Forms.Label label17;
        private System.Windows.Forms.TextBox txtBackupFilePath;
        #endregion

        private TextBox txtRemoteMeasure;
        private TextBox txtLocalMeasure;
        private Label label16;
        private Label label15;
        private Button btnSelectBackupFile;
        private Button btnSelectRestoreFile;
        private Button btnBackupToFile;
        private Button btnRestoreFromFile;
        private Button btnFillData;
        private Button btnCancelSync;
        private TextBox textBox1;
        private TextBox txtMinNumber;
        private NumericUpDown numMin;
        private NumericUpDown numMax;
        private TextBox txtTimeInterval;
        private Label label23;
        private NumericUpDown numWriteBatchSize;
        private Label labelWriteBatchSize;
        private NumericUpDown numMigrationConcurrency;
        private Label labelMigrationConcurrency;
        private NumericUpDown numSyncWindowHours;
        private Label labelSyncWindowHours;
    }

}
