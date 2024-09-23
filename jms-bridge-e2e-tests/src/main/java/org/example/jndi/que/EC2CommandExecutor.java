package org.example.jndi.que;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

public class EC2CommandExecutor {

    public static void main(String[] args) {
        String pemFilePath = "/Users/ganeshprabhu/gpbase/Psy/aws/LVM/pave-team-2024-000.pem";;
        String ec2User = "ec2-user";
        String ec2Host = "18.226.163.54";

        // List of commands to be executed on the EC2 instance
        List<String> commands = new ArrayList<>();
        commands.add("ls -l");
        commands.add("whoami");
        //commands.add("df -h");

        for (String command : commands) {
            executeCommand(pemFilePath, ec2User, ec2Host, command);
        }
    }

    private static void executeCommand(String pemFilePath, String ec2User, String ec2Host, String command) {
        String sshCommand = String.format("ssh -i %s %s@%s '%s'", pemFilePath, ec2User, ec2Host, command);

        try {
            ProcessBuilder processBuilder = new ProcessBuilder("bash", "-c", sshCommand);
            processBuilder.redirectErrorStream(true);

            Process process = processBuilder.start();

            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }

            int exitCode = process.waitFor();
            System.out.println("Command exited with code: " + exitCode);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

