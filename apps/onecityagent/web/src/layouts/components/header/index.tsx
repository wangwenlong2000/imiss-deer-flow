import { Layout } from 'antd';
import Toolbar from '../right-toolbar';

const { Header } = Layout;

const RagHeader = () => {
  // const {
  //   token: { colorBgContainer },
  // } = theme.useToken();
  // const navigate = useNavigateWithFromState();
  // const { pathname } = useLocation();
  // const { t } = useTranslate('header');
  // const appConf = useFetchAppConf();

  // const tagsData = useMemo(
  //   () => [
  //     { path: '/knowledge', name: t('knowledgeBase'), icon: KnowledgeBaseIcon },
  //     { path: '/chat', name: t('chat'), icon: MessageOutlined },
  //     // { path: '/search', name: t('search'), icon: SearchOutlined },
  //     { path: '/flow', name: t('flow'), icon: GraphIcon },
  //     // { path: '/file', name: t('fileManager'), icon: FileIcon }, //tys 文件菜单隐藏
  //   ],
  //   [t],
  // );

  // const currentPath = useMemo(() => {
  //   return (
  //     tagsData.find((x) => pathname.startsWith(x.path))?.name || 'knowledge'
  //   );
  // }, [pathname, tagsData]);

  // const handleChange = useCallback(
  //   (path: string): MouseEventHandler =>
  //     (e) => {
  //       e.preventDefault();
  //       navigate(path);
  //     },
  //   [navigate],
  // );

  // const handleLogoClick = useCallback(() => {
  //   navigate('/');
  // }, [navigate]);

  return (
    <Header
      style={{
        padding: '0',
        background: 'transparent',
        display: 'flex',
        justifyContent: 'flex-end',
        alignItems: 'center',
        height: '0.64rem',
      }}
    >
      {/*//tys header跳转链接*/}
      {/* <a href={window.location.origin}> */}
      {/* <Space
          size={12}
          onClick={handleLogoClick}
          className={styles.logoWrapper}
        > */}
      {/*<img src="/logo.svg" alt="" className={styles.appIcon} />*/}
      {/*<span className={styles.appName}>{appConf.appName}</span>*/}
      <div
        style={{
          width: '0.6rem',
          height: '0.6rem',
        }}
      ></div>
      {/* </Space> */}
      {/* </a> */}
      {/* <Space size={[0, 8]} wrap>
        <Radio.Group
          defaultValue="a"
          buttonStyle="solid"
          className={styles.radioGroup}
          value={currentPath}
        >
          {tagsData.map((item) => (
            <Radio.Button value={item.name} key={item.name}>
              <a href={item.path}>
                <Flex
                  align="center"
                  gap={8}
                  onClick={handleChange(item.path)}
                  className="cursor-pointer"
                >
                  <item.icon
                    className={styles.radioButtonIcon}
                    stroke={item.name === currentPath ? 'black' : 'white'}
                  ></item.icon>
                  {item.name}
                </Flex>
              </a>
            </Radio.Button>
          ))}
        </Radio.Group>
      </Space> */}
      <Toolbar></Toolbar>
    </Header>
  );
};

export default RagHeader;
