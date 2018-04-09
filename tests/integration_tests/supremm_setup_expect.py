import pexpect
import sys

def main():
    with open("supremm_expect_log", "w") as f:
        p = pexpect.spawn('supremm-setup')
        p.logfile = f
        
        p.expect("Select an option")
        p.sendline("c")
        
        p.expect("Enter path to configuration files")
        p.sendline()
        p.expect("Do you wish to specify the XDMoD install directory")
        p.sendline()
        p.expect("XDMoD configuration directory path")
        p.sendline()
        p.expect("Temporary directory to use for job archive processing")
        p.sendline()

        while True:
            i = p.expect(["Overwrite config file", "frearson", "mortorq", "phillips", "pozidriv", "robertson" ])
            p.sendline()
            if i != 0:
                p.expect("Directory containing node-level PCP archives")
                p.sendline()
                p.expect("Source of accounting data")
                p.sendline()
                p.expect("node name unique identifier")
                p.sendline()
                p.expect("Directory containing job launch scripts")
                p.sendline()
            else:
                break

        p.expect("Press ENTER to continue")
        p.sendline()

        p.expect("Select an option")
        p.sendline("d")
        p.expect("Enter path to configuration files")
        p.sendline()
        p.expect("DB hostname")
        p.sendline()
        p.expect("DB port")
        p.sendline()
        p.expect("DB Admin Username")
        p.sendline()
        p.expect("DB Admin Password")
        p.sendline()
        p.expect("Do you wish to proceed")
        p.sendline("y")
        p.expect("Press ENTER to continue")
        p.sendline()

        p.expect("Select an option")
        p.sendline("m")
        p.expect("Enter path to configuration files")
        p.sendline()
        p.expect("URI")
        p.sendline()
        p.expect("Do you wish to proceed")
        p.sendline("y")
        p.expect("Press ENTER to continue")
        p.sendline()
        
        p.expect("Select an option")
        p.sendline("q")

if __name__ == '__main__':
    main()
