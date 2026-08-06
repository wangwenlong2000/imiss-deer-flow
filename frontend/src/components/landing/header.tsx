import Image from "next/image";
import Link from "next/link";

export function Header() {
  return (
    <header className="fixed top-0 right-0 left-0 z-20 h-24 backdrop-blur-xs">
      <div className="flex h-full items-center px-6 md:px-8">
        <Link href="/">
          <div className="flex items-center">
            <Image
              src="/images/china-mobile-logo.png"
              alt="中国移动"
              width={2000}
              height={500}
              className="h-[4.75rem] w-auto md:h-[5.25rem]"
              priority
            />
          </div>
        </Link>
      </div>
      <hr className="from-border/0 via-border/70 to-border/0 absolute top-24 right-0 left-0 z-10 m-0 h-px w-full border-none bg-linear-to-r" />
    </header>
  );
}
